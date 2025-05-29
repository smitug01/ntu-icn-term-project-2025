#!/usr/bin/env python3
import socket
import os
import sys
import threading
import time
import mimetypes
import json
from datetime import datetime

# Configuration
HOST = '127.0.0.1'  # localhost
PORT = 8001  # Port for backend server 1
BUFFER_SIZE = 4096
SERVER_NAME = "Backend-Server-1"  # Identifies which backend is responding

# Define the directory where HTML files are stored
DOCUMENT_ROOT = os.path.dirname(os.path.abspath(__file__))

def get_content_type(file_path):
    """Determine content type based on file extension"""
    content_type, _ = mimetypes.guess_type(file_path)
    if content_type:
        return content_type
    return 'application/octet-stream'  # Default content type

def handle_api_request(uri):
    """Handle API requests and return appropriate response"""
    if uri == '/proxy-cgi/trace':
        # Create JSON response with server information
        server_info = {
            "server_name": SERVER_NAME,
            "port": PORT,
            "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "host": HOST
        }
        
        # Convert to JSON string
        response_body = json.dumps(server_info)
        
        # Create HTTP response
        response = "HTTP/1.1 200 OK\r\n"
        response += "Content-Type: application/json\r\n"
        response += f"Content-Length: {len(response_body)}\r\n"
        response += f"Server: {SERVER_NAME}\r\n"
        response += f"Date: {datetime.now().strftime('%a, %d %b %Y %H:%M:%S GMT')}\r\n"
        response += "Access-Control-Allow-Origin: *\r\n"  # Allow cross-origin requests
        response += "Connection: close\r\n\r\n"
        response += response_body
        
        return response.encode()
    
    return None  # Not an API request

def handle_client(client_socket, client_address):
    """Handle client connections"""
    print(f"[{SERVER_NAME}] Connection from {client_address}")
    
    try:
        # Receive the HTTP request
        request_data = client_socket.recv(BUFFER_SIZE).decode('utf-8')
        if not request_data:
            return
        
        # Parse the first line of the HTTP request: METHOD URI HTTP_VERSION
        request_line = request_data.split('\n')[0]
        method, uri, _ = request_line.split()
        
        # Check if this is an Trace request
        api_response = handle_api_request(uri)
        if api_response:
            client_socket.sendall(api_response)
            print(f"[{SERVER_NAME}] Trace Info: {uri}")
            return
        
        # Clean the URI to get the file path
        file_path = uri.strip('/')
        if file_path == '':
            file_path = 'index.html'  # Default file
        
        file_path = os.path.join(DOCUMENT_ROOT, file_path)
        
        # Check if file exists and serve it
        if os.path.isfile(file_path):
            # Get the file size
            file_size = os.path.getsize(file_path)
            
            # Determine content type
            content_type = get_content_type(file_path)
            
            # Create HTTP response header
            response_header = f"HTTP/1.1 200 OK\r\n"
            response_header += f"Content-Type: {content_type}\r\n"
            response_header += f"Content-Length: {file_size}\r\n"
            response_header += f"Server: {SERVER_NAME}\r\n"  # Add server identifier
            response_header += f"Date: {datetime.now().strftime('%a, %d %b %Y %H:%M:%S GMT')}\r\n"
            response_header += "Connection: close\r\n\r\n"
            
            # Send the header
            client_socket.sendall(response_header.encode())
            
            # Send the file content
            with open(file_path, 'rb') as file:
                client_socket.sendall(file.read())
                
            print(f"[{SERVER_NAME}] Served: {file_path}")
            
        else:
            # File not found - send 404 response
            response = "HTTP/1.1 404 Not Found\r\n"
            response += f"Server: {SERVER_NAME}\r\n"
            response += "Content-Type: text/html\r\n"
            response += "Connection: close\r\n\r\n"
            response += f"<!DOCTYPE HTML>\r\n<html>\r\n<head>\r\n"
            response += f"<title>404 Not Found</title>\r\n</head>\r\n"
            response += f"<body>\r\n<h1>404 Not Found</h1>\r\n"
            response += f"<p>The requested URL {uri} was not found on this server ({SERVER_NAME}).</p>\r\n"
            response += f"</body>\r\n</html>"
            
            client_socket.sendall(response.encode())
            print(f"[{SERVER_NAME}] 404 Not Found: {file_path}")
    
    except Exception as e:
        print(f"[{SERVER_NAME}] Error: {e}")
    
    finally:
        client_socket.close()

def start_server():
    """Start the web server"""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind((HOST, PORT))
        server_socket.listen(5)
        print(f"[{SERVER_NAME}] Server started at http://{HOST}:{PORT}")
        print(f"[{SERVER_NAME}] Trace info available at http://{HOST}:{PORT}/proxy-cgi/trace")
        
        while True:
            client_socket, client_address = server_socket.accept()
            client_thread = threading.Thread(target=handle_client, args=(client_socket, client_address))
            client_thread.daemon = True
            client_thread.start()
            
    except KeyboardInterrupt:
        print(f"[{SERVER_NAME}] Server is shutting down...")
    except Exception as e:
        print(f"[{SERVER_NAME}] Error: {e}")
    finally:
        server_socket.close()

if __name__ == "__main__":
    start_server()