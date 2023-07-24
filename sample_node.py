import argparse
import asyncio
import random

import raftos


async def run(node_id):
    names = []
    replicateds = []

    while True:
        # We can also check if raftos.get_leader() == node_id
        await raftos.wait_until_leader(node_id)
        command = input("input command: ")
        operation, operand = command.split()
        name, value = operand.split("=")

        index = names.index(name) if name in names else -1
        if index < 0:
            names.append(name)
            replicated = raftos.Replicated(name)
            replicateds.append(replicated)
        else:
            replicated = replicateds[index]

        if operation == "set":
            await replicated.set(value)
        elif operation == "get" and index >= 0:
            value = await replicated.get()
            print(value)
        elif operation == "stop":
            pass
    # rand_number = raftos.Replicated(name='rand_number')

    # while True:
    #     # We can also check if raftos.get_leader() == node_id
    #     await raftos.wait_until_leader(node_id)
    #     await asyncio.sleep(2)

    #     current_id = random.randint(1, 1000)
    #     await rand_number.set(current_id)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--node')
    parser.add_argument('--cluster')
    args = parser.parse_args()

    cluster = ['127.0.0.1:{}'.format(port) for port in args.cluster.split(",")]
    node = '127.0.0.1:{}'.format(args.node)

    raftos.configure({
        'log_path': './',
        'serializer': raftos.serializers.JSONSerializer
    })

    loop = asyncio.get_event_loop()
    loop2 = asyncio.new_event_loop()
    loop.create_task(raftos.register(node, cluster=cluster))
    loop2.run_until_complete(run(node))