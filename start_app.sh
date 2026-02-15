#!/bin/bash

# Heavy Aggregator Startup Script

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}--- Heavy Aggregator Startup ---${NC}"

# 1. Python Environment
echo -e "${GREEN}[1/4] Checking Python Dependencies...${NC}"
if [ -f "requirements.txt" ]; then
    pip3 install -r requirements.txt > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "Python dependencies installed."
    else
        echo "Warning: pip3 install failed. Trying pip..."
        pip install -r requirements.txt > /dev/null 2>&1
    fi
else
    echo "requirements.txt not found!"
    exit 1
fi

# 2. Node Environment
echo -e "${GREEN}[2/4] Checking Frontend Dependencies...${NC}"
cd webapp
if [ ! -d "node_modules" ]; then
    echo "Installing node_modules (this may take a moment)..."
    npm install
else
    echo "node_modules found."
fi

# 3. Start Backend
echo -e "${GREEN}[3/4] Starting Backend Server...${NC}"
cd ..
# Run python server in background, suppress standard logs but keep errors?
# Actually, let's just run it in background.
python3 server.py > backend.log 2>&1 &
BACKEND_PID=$!
echo "Backend running (PID: $BACKEND_PID). Logs in backend.log"

# 4. Start Frontend
echo -e "${GREEN}[4/4] Starting Frontend...${NC}"
cd webapp
# npm run dev usually outputs network address.
# We'll run it in background too, or just let it run?
# If we run it in background, we can wait for both.

npm run dev > ../frontend.log 2>&1 &
FRONTEND_PID=$!
echo "Frontend running (PID: $FRONTEND_PID). Logs in frontend.log"

# Wait a moment for services to spin up
sleep 3

echo -e "${BLUE}--- App Running! ---${NC}"
echo -e "Frontend: ${GREEN}http://localhost:5173${NC}"
echo -e "Backend:  ${GREEN}http://localhost:8000${NC}"
echo ""
echo "Press [CTRL+C] to stop everything."

# Open Browser
open http://localhost:5173

# Trap SIGINT to kill processes
trap "kill $BACKEND_PID $FRONTEND_PID; exit" SIGINT

# Wait forever
wait
