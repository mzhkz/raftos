import asyncio
import functools
import random

from .conf import config
from .exceptions import NotALeaderException
from .storage import FileStorage, Log, StateMachine
from .timer import Timer

def validate_term(func):


    @functools.wraps(func)
    def on_receive_function(self, data):
        if self.storage.term < data['term']:
            self.storage.update({
                'term': data['term']
            })
            # 自分より新しいタームを観測したら、リーダー辞退する。
            if not isinstance(self, Follower): 
                self.state.to_follower()

        if self.storage.term > data['term'] and not data['type'].endswith('_response'):
            response = {
                'type': '{}_response'.format(data['type']),
                'term': self.storage.term,
                'success': False,
                'request_id': data['request_id'] if data.get('request_id') else None
            }
            self.loop.ensure_future(self.state.send(response, data['sender']))
            return
        return func(self, data)
    return on_receive_function


def validate_commit_index(func):

    @functools.wraps(func)
    def wrapped(self, *args, **kwargs):
        # 最後に適応したログから、フォロワー間でマジョリティがとれたログのインデックスまで、ログを適応する。
        for not_applied in range(self.log.last_applied + 1, self.log.commit_index + 1):
            self.state_machine.apply(self.log[not_applied]['command'])
            # 適応したログのインデックスを更新する。match indexはリーダーと同期できている最新termのindexを指す。
            # commit indexはフォロワー間でマジョリティがとれたログのインデックスを指す。
            # match indexとcommit indexの違いは、match indexは個別のフォロワーがリーダーと同期できている最新termのindexを指すのに対し、commit indexはフォロワー間でマジョリティがとれたログのインデックスを指す。
            self.log.last_applied += 1

            try:
                self.apply_future.set_result(not_applied)
            except (asyncio.InvalidStateError, AttributeError):
                pass
        return func(self, *args, **kwargs)
    return wrapped


class BaseState:
    
    def __init__(self, state):
        self.state = state
        
        self.storage = self.state.storage
        self.log = self.state.log
        self.state_machine = self.state.state_machine

        self.id = self.state.id
        self.loop = self.state.loop

    
    @validate_term
    def on_receive_request_vote(self, data):
        pass

    @validate_term
    def on_receive_append_entries(self, data):
        pass

    @validate_term
    def on_receive_append_entries_response(self ,data):
        pass

class Leader(BaseState):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.heatbeat_timer = Timer(config.heatbeat_interval, self.heatbeat)
        self.stop_down_timer = Timer(config.step_down_missed_heartbeats * config.heartbeat_interval,
            self.state.to_follower)
        
        self.request_id = 0
        self.response_map = {}

    def start(self):
        self.init_log()
        self.heatbeat()
        self.heatbeat_timer.start()
        self.stop_down_timer.start()

    def stop(self):
        self.heatbeat_timer.stop()
        self.stop_down_timer.stop()

    def init_log(self):
        self.log.next_index = {
            follower: self.log.last_log_index + 1 for follower in self.state.cluster
        }

        self.log.match_index = {
            follower: 0 for follower in self.state.cluster
        }

    async def append_entries(self, destination=None):
        destination_list = [destination] if destination else self.state.cluster
        for destination in destination_list:
            data = {
                'type': 'append_entries',

                'term': self.storage.term,
                'leader_id': self.id,
                'commit_index': self.log.commit_index,

                'request_id': self.request_id
            }

            next_index = self.log.next_index[destination] # 各followerごとに状態を管理
            prev_index = next_index - 1

            #自分のログよりも、Followerのログが遅れていたら、ログを送信する。Conflictしたログを持っていた場合は、line:151でログのインデックスを戻しているので、ここでリーダーのログに合わせる。
            if self.log.last_log_index >= next_index:
                data['entries'] = [self.log[next_index]]
            else: #そうでなければなにもしない。
                data['entires'] = []

            data.update({
                'prev_log_index': prev_index,
                'prev_log_term': self.log[prev_index]['term'] if self.log and prev_index else 0
            })

            self.loop.ensure_future(self.state.send(data, destination))
    
    @validate_commit_index
    @validate_term
    def on_receive_append_entries_response(self, data):
        sender_id = self.state.get_sender_id(data['sender'])

        if data['request_id'] in self.response_map:
            self.response_map[data['request_id']].add(sender_id) # 返信者を追加

            # 今受け取った返事で半数以上のコミットが確認できた。
            if self.state.is_majority(len(self.response_map[data['request_id']]) + 1):
                self.stop_down_timer.reset()
                del self.response_map[data['request_id']]
        
        if not data['success']:
            #一貫性チェックに失敗した場合は、前のログのとの一貫性がない、すなわちフォロワーがリーダーが送信したログを持っていないということなので、ログを一つ戻す。
            self.log.next_index[sender_id] = max(self.log.next_index[sender_id] - 1, 1) 

        else:
            # フォロワーによる一貫性チェックに成功した場合は、フォロワーが合意したログのインデックスを更新する。
            if data['last_log_index'] > self.log.match_index[sender_id]:
                self.log.next_index[sender_id] = data['last_log_index'] + 1
                self.log.match_index[sender_id] = data['last_log_index'] # match indexはリーダーと同期できている最新termのindexを指す。

            # ここでフォロワーノード間でマジョリティがとれているか判断し、コミットする。またself.log.commit_indexを更新する。
            self.update_commit_index()
        
        # もしフォロワーのエントリーが最新ではなかった場合、ここでログを送信する。
        if self.log.last_log_index >= self.log.next_index[sender_id]:
            asyncio.ensure_future(self.append_entries(destination=sender_id), loop=self.loop)

    
    def update_commit_index(self):
        commited_on_majority = 0
        # フォロワーのログの中で、リーダーのログと一致しているものがあるか確認する。
        # 一致しているものがあれば、そのindexをコミット済みのindexとする。
        # 検索するインデックスのレンジは、コミット済みのindexよりも大きいものから、コミットしていない最新（キャッシュに入っている）のログまで。
        for index in range(self.log.commit_index + 1, self.log.last_log_index + 1):
            commited_count = len([1 for follower in self.log.match_index if self.log.match_index[follower] >= index])

            is_current_term = self.log[index]['term'] == self.storage.term
            # indexについて、マジョリティがとれていて、かつ、現在のタームであれば、コミット済みのindexを更新する。
            if self.state.is_majority(commited_count + 1) and is_current_term:
                commited_on_majority = index
            else:
                continue


        # すでに合意を取ったcommit_indexよりも大きいindexに対して、合意が取れている場合は、commit_indexを更新する。
        # 一度コミットしたものは覆さないので、条件はcommit_indexよりも大きいものにする。
        if commited_on_majority > self.log.commit_index:
            self.log.commit_index = commited_on_majority

    
    async def execute_command(self, command):
        self.apply_future = self.loop.create_future()

        entry = self.log.write(self.storage.term, command)
        self.loop.ensure_future(self.append_entries())

        # append_entriesのレスポンスが返ってくるまで待つ. 
        # append_entriesのレスポンスが返ってくると、apply_futureに値が入る。
        await self.apply_future

    def heatbeat(self):
        self.request_id += 1
        self.response_map[self.request_id] = set()
        self.loop.ensure_future(self.append_entries())

class Candidate(BaseState):


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.election_timer = Timer(self.election_interval, self.state.to_follower)
        self.vote_count = 0

    def start(self):
        self.storage.update({
            'term': self.storage.term + 1,
            'voted_for': self.id
        })

        self.vote_count = 1
        self.request_vote()
        self.election_timer.start()

    def stop(self):
        self.election_timer.stop()

    def request_vote(self):
        data = {
            'type': 'request_vote',
            'candidate_id': self.id,
            'last_log_index': self.log.last_log_index,
            'last_log_term': self.log.last_log_term
        }

        self.state.broadcast(data)

    @validate_term
    def on_receive_append_entries_response(self, data):
        if data.get('vote_granted'):
            self.vote_count += 1

            if self.state.is_majority(self.vote_count):
                self.state.to_leader()

    @validate_term
    def on_receive_append_entries(self, data):
        # 同じタームを持っているCandidateがいた場合、followerに格下げ
        if self.state.storage.term == data['term']:
            self.state.to_follower()

    @staticmethod
    def election_interval():
        return random.uniform(*config.election_interval)

class Follower(BaseState):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.election_timer = Timer(self.election_interval)

    def start(self):
        self.election_timer.start()
    
    def stop(self):
        self.election_timer.stop()

    def init_storage(self):
        if not self.storage.exists('term'):
            self.storage.update({
                'term': 0,
            })
        self.storage.update({
            'voted_for': None
        })


    @staticmethod
    def election_interval():
        return random.uniform(*config.election_interval)
    
    @validate_commit_index
    @validate_term
    def on_receive_append_entries(self, data):
        self.state.set_leader(data['leader_id'])

        try:
            prev_log_index = data['prev_log_index']
            
            # 最後のログと一貫性があるか、タームは合っているか
            if prev_log_index > self.log.last_log_index or (prev_log_index and self.log[prev_log_index]['term'] != data['prev_log_term']):
                response = {
                    'type': 'append_entries_response',
                    'term': self.storage.term,
                    'success': False,

                    'request_id': data['request_id']
                }
                self.loop.ensure_future(self.state.send(response, data['sender']))
                return
        except IndexError:
            pass

        is_append = True
        new_index = data['prev_log_index'] + 1
        try:
            if self.log[new_index]['term'] != data['entires'][0]['term']:
                self.log.erase_from(new_index)
            else:
                is_append = False
        except IndexError:
            pass

        if is_append:
            for entry in data['entries']:
                self.log.write(entry['term'], entry['command'])
        
        if self.log.commit_index < data['commit_index']:
            self.log.commit_index = min(data['commit_index'], self.log.last_log_index)
        
        response = {
            'type': 'append_entries_response',
            'term': self.storage.term,
            'success': True,

            'last_log_index': self.log.last_log_index,
            'request_id': data['request_id']
        }

        self.loop.ensure_future(self.state.send(response, data['sender']))

        self.election_timer.reset()

    @validate_term
    def on_receive_request_vote(self, data):
        if self.storage.voted_for is None and not data['type'].endswith('_response'):

            # 自分より大きいタームかインデックスをCandidateが持っていた場合、Candidateを投票する
            if data['last_log_term'] != self.log.last_log_term:
                up_to_date = data['last_log_term'] > self.log.last_log_term
            else:
                up_to_date = data['last_log_index'] >= self.log_last_log_index

            if up_to_date:
                self.storage.update({
                    'voted_for': data['candidate_id']
                })
            response = {
                'type': 'request_vote_response',
                'term': self.storage.term,
                'vote_granted': up_to_date
            }

            self.loop.ensure_future(self.state.send(response, data['sender']))

    def start_election(self):
        self.state.to_candidate()


def leader_required(func):

    @functools.wraps(func)
    async def wrapped(cls, *args, **kwargs):
        await cls.wait_for_election_success() #選挙中だったら待つ
        if not isinstance(cls.leader, Leader): # リーダじゃなければ例外をはく
            raise NotALeaderException(
                'Leader is {}!'.format(cls.leader or 'not chosen yet')
            )

        return await func(cls, *args, **kwargs)
    return wrapped


class State:
    leader = None
    leader_future = None

    wait_until_leader_id = None
    wait_until_leader_future = None

    def __init__(self, server):
        self.server = server
        self.id = self._get_id(server.host, server.port)
        self.__class__.loop = self.server.loop

        self.storage = FileStorage(self.id)
        self.log = Log(self.id)
        self.state_machine = StateMachine(self.id)

        self.state = Follower(self)

    def start(self):
        self.state.start()

    def stop(self):
        self.state.stop()

    @classmethod
    @leader_required
    async def get_value(cls, name):
        return cls.leader.state_machine[name]
    

    @classmethod
    @leader_required
    async def set_value(cls, name, value):
        await cls.leader.execute_command({name: value})

    def send(self, data, destination):
        return self.server.send(data, destination)
    
    def broadcast(self, data):
        return self.server.broadcast(data)
    
    def request_handler(self, data):
        getattr(self.state, 'on_receive_{}'.format(data['type']))(data)

    @staticmethod
    def _get_id(host, port):
        return '{}:{}'.format(host, port)
    
    def get_sender_id(self, sender):
        return self._get_id(*sender)
    
    @property
    def cluster(self):
        return [self._get_id(*address) for address in self.server.cluster]
    
    def is_majority(self, count):
        return count > (self.server.cluster_count // 2)
    
    def to_candidate(self):
        self._change_state(Candidate)
        self.set_leader(None)


    def to_leader(self):
        self._change_state(Leader)
        self.set_leader(self.state)


    def to_follower(self):
        self._change_state(Follower)
        self.set_leader(None)
    
    def set_leader(self, leader):
        cls = self.__class__ # cls classの略, leaderはclass変数とでもいいたいのか
        cls.leader = leader

        if cls.leader and cls.leader_future and not cls.leader_future.done():
            cls.leader_future.set_result(cls.leader)

        if cls.wait_until_leader_id and (cls.wait_until_leader_future and not cls.wait_until_leader_future.done()):
            cls.wait_until_leader_future.set_result(cls.leader)

    
    def _change_state(self, new_state):
        self.state.stop()
        self.state = new_state(self)
        self.state.start()

    @classmethod
    def get_leader(cls):
        if isinstance(cls.leader, Leader):
            return cls.leader.id
        
        return cls.leader
    

    @classmethod
    async def wait_for_election_success(cls):
        if cls.leader is None:
            cls.leader_future = cls.loop.create_future() # 新しいfutureobjectを作成して、それが実行し終えるんまで待つ
            await cls.leader_future

    @classmethod
    async def await_until_leader(cls, node_id):
        if node_id is None:
            raise ValueError("Node id can be None!")
        
        if cls.get_leader() != node_id:
            cls.wait_until_leader_id = node_id
            cls.wait_until_leader_future = cls.loop.create_future()
            await cls.wait_until_leader_future

            cls.wait_until_leader_id = None
            cls.wait_until_leader_future = None



