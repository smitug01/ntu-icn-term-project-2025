# Load Balancer Project

A simple web server system with load balancing capabilities.

## Components

- `load_balancer.py` - Main load balancer that distributes traffic
- `backend_server1.py` & `backend_server2.py` - Backend web servers
- `index.html` & `helloworld.html` - Sample web pages

## Getting Started

1. Start the backend servers:
```
python backend_server1.py
python backend_server2.py
```

2. Start the load balancer:
```
python load_balancer.py
```

3. Access the website at http://localhost:8000