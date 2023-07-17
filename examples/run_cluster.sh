# Add raftos package to PYTHONPATH so we don't need to install it to run example
# Remove previous data
rm -f *.log
rm -f *.storage
rm -f *.state_machine

# Start
python sample_node.py --node "8000" --cluster "8000 8001 8002" &
python sample_node.py --node "8001" --cluster "8000 8001 8002" &
python sample_node.py --node "8002" --cluster "8000 8001 8002" &
