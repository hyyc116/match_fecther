#coding:utf-8
'''
抓取比赛

'''

from urllib import request
import time
import json

KEY = '03E4F14C833104E784736E15EDC1A6E7'


def craw_matches(start_at_match_id=None):

    url = 'http://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/v1?min_players=10&key={}&matches_requested=10'.format(KEY)

    if start_at_match_id is not None:
        url+='&start_at_match_id={}'.format(start_at_match_id)

    with request.urlopen(url) as f:
        data = f.read().decode('utf-8')

        dataObj = json.loads(data)

        print('total:',dataObj['result']['total_results'])

        print('remaining:',dataObj['result']['results_remaining'])

        matches = dataObj['result']['matches']


        for i,match in enumerate(matches):

            match_id = match['match_id']

            #转换为localtime
            time_local = time.localtime(match['start_time'])
            #转换为新的时间格式
            dt = time.strftime("%Y-%m-%d %H:%M:%S",time_local)

            print(i,'the match id is',match_id,',start at',dt,'seq num:',match['match_seq_num'])

    return match_id,dt

def local_time(timestamp):

    #转换为localtime
    time_local = time.localtime(timestamp)
    #转换为新的时间格式
    dt = time.strftime("%Y-%m-%d %H:%M:%S",time_local)

    return dt

def iter_fetch():

    start_match_id = None

    while True:

        start_match_id,dt =  craw_matches(start_match_id)

        print(start_match_id,dt)

        time.sleep(1)

def craw_match_by_seqnum(start_at_match_seq_num=None):

    url = 'http://api.steampowered.com/IDOTA2Match_570/GetMatchHistoryBySequenceNum/v1?key={}&matches_requested=100'.format(KEY)

    if start_at_match_seq_num is not None:
        url+='&start_at_match_seq_num={}'.format(start_at_match_seq_num)

    lines = []

    with request.urlopen(url) as f:
        data = f.read().decode('utf-8')

        dataObj = json.loads(data)

        matches = dataObj['result']['matches']

        for match in matches:

            match_id = match['match_id']

            seq_num = match['match_seq_num']

            dt = local_time(match['start_time'])

            lines.append(json.dumps(match))


    return seq_num,dt,lines

def iter_fetch_seq(seq_num=None):

    matches = []

    count = 0
    progress = 0
    while True:
        seq_num,dt,lines = craw_match_by_seqnum(seq_num)

        progress+=1

        if progress%100==0:

            print('progress {}, seq num:{}, time:{}, {} matches crawed ..'.format(progress,seq_num,dt,count))


        matches.extend(lines)

        if len(matches)>1000:
            open('matches.txt','a').write('\n'.join(matches)+'\n')
            open('latest_seqnum.txt','a').write('{}\n'.format(seq_num))

            count+=len(matches)
            matches=[]

            time.sleep(60)

        time.sleep(15)

    if len(matches)>0:
        open('matches.txt','a').write('\n'.join(matches)+'\n')




def craw_match_detail():

    pass


if __name__ == '__main__':
    # iter_fetch()

    # seq_num = '4309000000'

    seq_num = str(open('latest_seqnum.txt').readlines()[-1].strip())
    print('starting fetch from seqnum {} ...'.format(seq_num))
    iter_fetch_seq(seq_num)




