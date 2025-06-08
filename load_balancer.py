#!/usr/bin/env python3
import socket
import os
import sys
import time
import re
from urllib.parse import urlparse
import uuid  # Add this for unique cache keys
import errno

# Configuration
HOST = '127.0.0.1'  # Localhost
PORT = 8000  # Port to listen on
BUFFER_SIZE = 4096  # Socket buffer size
TIMEOUT = 5  # Socket timeout in seconds

# Backend servers configuration
BACKEND_SERVERS = [
    ('127.0.0.1', 8001),  # backend_server1
    ('127.0.0.1', 8002)   # backend_server2
]

CACHE_DIR = "cache"
STICKY_COOKIE_NAME = "sticky_backend"
DEBUG = True

# Endpoints that should not be cached
NO_CACHE_ENDPOINTS = [
    '/proxy-cgi/trace'
]

# Ensure cache directory exists
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

class LoadBalancer:
    def __init__(self, host, port, backend_servers):
        """Initialize the load balancer with host, port, and backend servers."""
        self.host = host
        self.port = port
        self.backend_servers = backend_servers
        self.current_backend_index = 0
        
    def start(self):
        """Start the load balancer server."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            server_socket.bind((self.host, self.port))
            server_socket.listen(5)
            print(f"Load balancer running on {self.host}:{self.port}")
            print(f"Backend servers: {self.backend_servers}")
            
            while True:
                client_conn, client_addr = server_socket.accept()
                print(f"Connection from {client_addr}")
                self.handle_client(client_conn)
                
        except KeyboardInterrupt:
            print("Shutting down load balancer...")
        finally:
            server_socket.close()
            
    def handle_client(self, client_conn):
        """Handle client connection."""
        try:
            # Receive client request
            request_data = self.receive_all(client_conn)
            if not request_data:
                return
            
            # Parse the HTTP request
            method, path, headers = self.parse_request(request_data)
            if not method or not path:
                return
            
            # Extract filename from path for caching purposes
            filename = self.get_filename_from_path(path)
            
            # Generate a unique cache key that includes the full path
            cache_key = path if path != '/' else '/index.html'
            cache_file = os.path.join(CACHE_DIR, f"{filename.replace('/', '_')}.cache")
            
            # Check if endpoint should be cached
            should_cache = self.should_cache_endpoint(path)
            
            # Log cookie information for debugging
            if DEBUG:
                print(f"Request headers: {headers}")
                if 'Cookie' in headers:
                    print(f"Cookie header: {headers['Cookie']}")
                
                if should_cache:
                    print(f"Endpoint {path} will be cached")
                else:
                    print(f"Endpoint {path} will NOT be cached")
            
            # Check cache only if endpoint is cacheable
            use_cache = False
            if should_cache and os.path.exists(cache_file):
                print(f"Cache Hit for {cache_key}")
                # Read cached response and send to client
                with open(cache_file, 'rb') as f:
                    cached_response = f.read()
                
                # Use the cached response
                use_cache = True
                client_conn.sendall(cached_response)
                return
            elif should_cache:
                print(f"Cache Miss for {cache_key}")
            
            # Check for sticky session cookie
            backend_server = self.get_backend_from_cookie(headers)
            should_set_cookie = False
            
            if backend_server:
                if self.is_backend_available(backend_server):
                    # Use the backend from the cookie
                    selected_backend = backend_server
                    print(f"Using sticky backend: {selected_backend}")
                else:
                    print(f"Sticky backend {backend_server} is unavailable")
                    selected_backend = self.select_backend_round_robin()
                    should_set_cookie = True
                    print(f"Selected new backend (round-robin): {selected_backend}")
            else:
                # Use round-robin to select backend
                selected_backend = self.select_backend_round_robin()
                should_set_cookie = True
                print(f"Selected backend (round-robin): {selected_backend}")
            
            # Forward the request to the selected backend
            response_data = self.forward_request(selected_backend, request_data)

            # If the response is a timeout, send 504 Gateway Timeout
            if response_data == b'TIMEOUT':
                print(f"Timeout while connecting to backend {selected_backend}")
                self.send_error(client_conn, 504, "Gateway Timeout")
                return
            
            # If no response from backend, send 502 Bad Gateway
            if not response_data:
                print(f"No response from backend {selected_backend}, sending 502 Bad Gateway")
                self.send_error(client_conn, 502, "Bad Gateway")
                return
            
            # Check if we need to add a Set-Cookie header
            if should_set_cookie and response_data:
                if self.is_success_response(response_data):
                    print(f"Adding sticky session cookie for {selected_backend}")
                    response_data = self.add_cookie_header(response_data, selected_backend)
                    
                    # Log the modified response headers for debugging
                    if DEBUG:
                        try:
                            headers_end = response_data.find(b'\r\n\r\n')
                            if headers_end != -1:
                                headers_str = response_data[:headers_end].decode('utf-8', errors='ignore')
                                print(f"Modified response headers: {headers_str}")
                        except Exception as e:
                            print(f"Error parsing response headers: {e}")
            
            # Cache successful responses for cacheable endpoints
            # if should_cache and response_data and self.is_success_response(response_data):
            #     with open(cache_file, 'wb') as f:
            #         f.write(response_data)
            #     print(f"Response cached to {cache_file}")
            if should_cache and response_data and self.is_success_response(response_data):
                try:
                    # Ensure cache directory exists
                    os.makedirs(CACHE_DIR, exist_ok=True)
                    with open(cache_file, 'wb') as f:
                        f.write(response_data)
                    print(f"Response cached to {cache_file}")
                except OSError as e:
                    # Handle specific cache write errors
                    if e.errno in (errno.ENOENT, errno.EACCES):
                        print(f"Cache write error] {e} → skip caching, still 200")
                    else:
                        raise
            
            # Send response back to client
            if response_data:
                client_conn.sendall(response_data)
            
        except Exception as e:
            print(f"Error handling client: {e}")
            self.send_error(client_conn, 502, "Bad Gateway")
        finally:
            client_conn.close()
    
    def should_cache_endpoint(self, path):
        """Determine if an endpoint should be cached."""
        # Don't cache API endpoints or other dynamic content
        return path not in NO_CACHE_ENDPOINTS
            
    def receive_all(self, conn):
        """Receive all data from the connection."""
        conn.settimeout(TIMEOUT)
        data = b''
        try:
            while True:
                chunk = conn.recv(BUFFER_SIZE)
                if not chunk:
                    break
                data += chunk
                # If we've received the full HTTP headers and no body is expected
                if b'\r\n\r\n' in data and not (
                    b'Content-Length:' in data or 
                    b'Transfer-Encoding: chunked' in data
                ):
                    break
        except socket.timeout:
            print("Socket timeout while receiving data")
            # Return a special bytes value instead of string
            return b'TIMEOUT'
        except Exception as e:
            print(f"Error receiving data: {e}")
            return None
        return data
    
    def parse_request(self, request_data):
        """Parse the HTTP request into method, path, and headers."""
        try:
            request_text = request_data.decode('utf-8', errors='ignore')
            request_lines = request_text.split('\r\n')
            
            if DEBUG:
                print(f"Request first line: {request_lines[0] if request_lines else 'No request lines'}")
            
            if not request_lines:
                return None, None, {}
                
            request_line = request_lines[0].split()
            
            if len(request_line) < 3:
                return None, None, {}
            
            method, request_uri, protocol = request_line
            
            # Parse headers
            headers = {}
            for line in request_lines[1:]:
                if not line:
                    break
                if ':' in line:
                    key, value = line.split(':', 1)
                    headers[key.strip()] = value.strip()
            
            # Extract path
            parsed_url = urlparse(request_uri)
            path = parsed_url.path if parsed_url.path else '/'
            
            return method, path, headers
        except Exception as e:
            print(f"Error parsing request: {e}")
            return None, None, {}
    
    def get_filename_from_path(self, path):
        """Extract filename from path for caching purposes."""
        if path == '/':
            return 'root_index.html'  # Cache root path as 'root_index.html'
        
        normalized = path.lstrip('/')  # Remove leading slash
        
        if path.endswith('/'):
            normalized = normalized.rstrip('/') + '_index'
        
        return normalized.replace('/', '_')
    
    def get_backend_from_cookie(self, headers):
        """Extract backend server from cookie header."""
        if 'Cookie' not in headers:
            print("No Cookie header found") if DEBUG else None
            return None
        
        cookies = headers['Cookie'].split(';')
        for cookie in cookies:
            cookie = cookie.strip()
            if DEBUG:
                print(f"Processing cookie: {cookie}")
                
            if cookie.startswith(f"{STICKY_COOKIE_NAME}="):
                value = cookie[len(f"{STICKY_COOKIE_NAME}="):]
                try:
                    host, port_str = value.split(':')
                    backend = (host, int(port_str))
                    print(f"Found backend in cookie: {backend}") if DEBUG else None
                    return backend
                except Exception as e:
                    print(f"Error parsing backend from cookie: {e}, value: {value}") if DEBUG else None
                    return None
        
        print(f"Cookie {STICKY_COOKIE_NAME} not found in cookies") if DEBUG else None
        return None
    
    def is_backend_available(self, backend):
        """Check if backend server is available."""
        host, port = backend
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1)
            s.connect((host, port))
            s.close()
            available = True
        except Exception as e:
            print(f"Backend {backend} not available: {e}") if DEBUG else None
            available = False
        return available
    
    def select_backend_round_robin(self):
        """Select a backend server using round-robin algorithm."""
        # Make sure we try all backends if some are unavailable
        for _ in range(len(self.backend_servers)):
            selected_backend = self.backend_servers[self.current_backend_index]
            self.current_backend_index = (self.current_backend_index + 1) % len(self.backend_servers)
            
            if self.is_backend_available(selected_backend):
                print(f"Round-robin selected backend: {selected_backend}")
                return selected_backend
        
        # If all backends are unavailable, return the first one (will be handled as error)
        print("All backends unavailable, returning first one")
        return self.backend_servers[0]
    
    def forward_request(self, backend, request_data):
        """Forward the request to the backend server."""
        host, port = backend
        try:
            # Connect to backend server
            backend_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            backend_socket.settimeout(TIMEOUT)
            backend_socket.connect((host, port))
            
            # Forward request
            backend_socket.sendall(request_data)
            print(f"Request forwarded to backend {host}:{port}")
            
            # Get response
            response_data = self.receive_all(backend_socket)
            backend_socket.close()
            
            if response_data and response_data != b'TIMEOUT':
                print(f"Received response from backend {host}:{port}")
                
                # Debug response status
                if DEBUG:
                    try:
                        status_line = response_data.split(b'\r\n')[0].decode('utf-8', errors='ignore')
                        print(f"Response status: {status_line}")
                    except:
                        pass
            elif response_data == b'TIMEOUT':
                print(f"Connection to backend {host}:{port} timed out")
                return b'TIMEOUT'
            else:
                print(f"No response from backend {host}:{port}")
                
            return response_data
        except socket.timeout:
            print(f"Connection to backend {host}:{port} timed out")
            return None
        except Exception as e:
            print(f"Error forwarding request to backend {host}:{port}: {e}")
            return None
    
    def is_success_response(self, response_data):
        """Check if the response is successful (200 OK)."""
        try:
            if isinstance(response_data, dict) or not isinstance(response_data, bytes):
                return False
            status_line = response_data.split(b'\r\n')[0].decode('utf-8', errors='ignore')
            return '200 OK' in status_line
        except Exception as e:
            print(f"Error checking response status: {e}")
            return False
    
    def add_cookie_header(self, response_data, backend):
        """Add Set-Cookie header to the response."""
        try:
            host, port = backend
            backend_str = f"{host}:{port}"
            cookie_header = f"Set-Cookie: {STICKY_COOKIE_NAME}={backend_str}; Path=/\r\n"
            
            # Insert cookie header before the blank line separating headers and body
            header_end = response_data.find(b'\r\n\r\n')
            if header_end != -1:
                # Extract existing headers for debugging
                if DEBUG:
                    existing_headers = response_data[:header_end].decode('utf-8', errors='ignore')
                    print(f"Existing headers: {existing_headers}")
                    print(f"Adding cookie header: {cookie_header.strip()}")
                
                new_response = response_data[:header_end] + \
                              f"\r\n{cookie_header}".encode() + \
                              response_data[header_end:]
                return new_response
            return response_data
        except Exception as e:
            print(f"Error adding cookie header: {e}")
            return response_data
    
    def send_error(self, conn, code, message):
        """Send an error response to the client."""
        try:
            html = f"""
            <!DOCTYPE HTML>
            <html>
            <head>
                <title>{code} {message}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; text-align: center; padding: 50px; }}
                    h1 {{ color: #d9534f; }}
                </style>
            </head>
            <body>
                <h1>{code} {message}</h1>
                <p>The load balancer encountered an error while processing your request.</p>
            </body>
            </html>
            """
            response = f"HTTP/1.1 {code} {message}\r\n"
            response += "Content-Type: text/html\r\n"
            response += f"Content-Length: {len(html)}\r\n"
            response += "\r\n"
            response += html
            
            conn.sendall(response.encode())
        except Exception as e:
            print(f"Error sending error response: {e}")
            pass


if __name__ == '__main__':
    lb = LoadBalancer(HOST, PORT, BACKEND_SERVERS)
    lb.start()