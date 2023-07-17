import argparse
import asyncio
from datetime import datetime
import random

import raftos


async def run(node_id):
    # Dict-like object: data.update(), data['key'] etc
    data = raftos.ReplicatedDict(name='data')


    while True:
        # We can also check if raftos.get_leader() == node_id
        await raftos.wait_until_leader(node_id)

        await asyncio.sleep(2)

        current_id = random.randint(1, 1000)
        data_map = {
            str(current_id): {
                'created_at': datetime.now().strftime('%d/%m/%y %H:%M')
            }
        }

        await data.update(data_map)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--node')
    parser.add_argument('--cluster')
    args = parser.parse_args()

    cluster = ['127.0.0.1:{}'.format(port) for port in args.cluster.split(',')]
    node = '127.0.0.1:{}'.format(args.node)

    raftos.configure({
        'log_path': './',
        'serializer': raftos.serializers.JSONSerializer
    })

    loop = asyncio.get_event_loop()
    loop.create_task(raftos.register(node, cluster=cluster))
    loop.run_until_complete(run(node))