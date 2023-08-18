import os, argparse,logging
from laceworksdk import LaceworkClient
from datetime import datetime, timedelta, timezone
import concurrent.futures
import pandas as pd
import json,re

MAX_THREADS=8

def get_resource_groups(lw_client):
    ls={}
    rgs=lw_client.resource_groups.get()['data']
    #print(rgs)
    for rg in rgs:
        if rg['resourceType']=='AWS':
            #print(rg['resourceName'])
            #print(rg['propsJson']['accountIds'])
            for account in rg['propsJson']['accountIds']:
                #ls.append([rg['resourceName'],account])
                if account not in ls.keys():
                    ls[account] = [rg['resourceName']]
                else:
                    ls[account].append(rg['resourceName'])
                #rec = { account : rg['resourceName'] }
                #ls.append(rec)
    return ls

def get_alerts(lw_client,query):
    results=[]
    alerts=lw_client.alerts.search(query)
    for h in alerts:
        results = results + h['data']
    return results

def get_alerts_all(lw_client,args):
    alerts=[]
    futures=[]
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_threads ) as pool:
        for i in range(args.days):
            d_start = (i+1)
            d_end=i
            start_time = (datetime.now(timezone.utc) - timedelta(days=(d_start))).strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time = (datetime.now(timezone.utc) - timedelta(days=d_end)).strftime('%Y-%m-%dT%H:%M:%SZ')
            query = {
            "timeFilter" : {
                "startTime" : start_time,
                "endTime" : end_time
            },
            "returns": [ "alertId", "severity" ] }
            futures.append(pool.submit(get_alerts,lw_client,query))
        for future in concurrent.futures.as_completed(futures):
            alerts=alerts + future.result()
    return alerts


def get_events(lw_client,query,rg):
    results=[]
    evt = lw_client.events.search(query)
    for h in evt:
        results = results + h['data']
    evts=process_events(results,rg)

    return evts


def get_events_all(lw_client,args,rg):
    events=[]
    futures=[]
    # create slice of time
    rg = get_resource_groups(lw_client)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_threads ) as pool:
        for i in range(args.days):
            d_start = (i+1)
            d_end=i
            start_time = (datetime.now(timezone.utc) - timedelta(days=(d_start))).strftime('%Y-%m-%dT%H:%M:%SZ')
            end_time = (datetime.now(timezone.utc) - timedelta(days=d_end)).strftime('%Y-%m-%dT%H:%M:%SZ')
            print("Getting event: [" + str(i) + "] start: " + start_time + " end: " + end_time )
            query = {
            "timeFilter" : {
                "startTime" : start_time,
                "endTime" : end_time
            }}
            futures.append(pool.submit(get_events,lw_client,query,rg))
        for future in concurrent.futures.as_completed(futures):
            events=events + future.result()


    return events

def process_events(events,rg):
    p_events=[]

    #print(json.dumps(events[0],indent=4))
    for evt in events:
        account="N/A"
        p_rg = ["N/A"]
        if 'srcEvent' in evt.keys():
            if 'recipientAccountId' in evt['srcEvent'].keys():
                #print('Extracted account:' + json.dumps(evt['srcEvent']['recipientAccountId'],indent=4))
                account=evt['srcEvent']['recipientAccountId']
            if 'accountId' in  evt['srcEvent'].keys() and account=="N/A":
                account=evt['srcEvent']['accountId']
            if 'account_id' in evt['srcEvent'].keys() and account=="N/A":
                account=evt['srcEvent']['account_id']
            if 'Id' in evt['srcEvent'].keys() and account=="N/A":
                account=evt['srcEvent']['Id']
            if 'accountcallee' in evt['srcEvent'].keys() and account=="N/A":
                account=evt['srcEvent']['accountcallee']
        if not account == "N/A" and account in rg.keys():
                p_rg = rg[account]
        #account =  evt['srcEvent'].get('recipientAccountId', False) or evt.get('accountId', False)
        ept = { 'account': account ,  'alertId':evt['id'] , 'ressource_groups': p_rg }
        p_events.append(ept)
        #print('Extracted account:' + json.dumps(evt,indent=4))

    return p_events

def get_file_name_base(lw_client):
    #print(lw_client.user_profile.get()['data'][0]['url'])
    try:
        client=re.search("^([^\.]+)",lw_client.user_profile.get()['data'][0]['url']).group()
    except:

        client='not-found'
    timestp=datetime.now(timezone.utc).strftime('%Y-%m-%d_%H.%M.%S')
    filename=client+'_ressource_groups_'+ timestp +'.xlsx'

    return filename

def main(args):
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARN)

    try:
        lw_client = LaceworkClient(
            account=args.account,
            subaccount=args.subaccount,
            api_key=args.api_key,
            api_secret=args.api_secret,
            profile=args.profile
        )
    except Exception:
        raise

    #get_resource_groups(lw_client)
    rg = get_resource_groups(lw_client)
    events=get_events_all(lw_client,args,rg)
    #print(events[0])
    events_df = pd.json_normalize(events,record_path=['ressource_groups'],meta=['alertId','account'])
    events_df.rename(columns={0:"ressource_group"},inplace=True)
    #print(events_df)
    alerts=get_alerts_all(lw_client,args)
    alerts_df = pd.json_normalize(alerts)
    g_df = pd.merge(events_df,alerts_df, how='left',on='alertId')
    # Grouping data
    g_df_sum = g_df.groupby(['ressource_group','severity']).count().sort_index()

    g_df_sum.drop(columns=['account'], inplace=True)
    g_df_sum.rename(columns={"alertId":"number"},inplace=True)
    print(g_df_sum)
    with pd.ExcelWriter(get_file_name_base(lw_client) + ".xlsx",engine='xlsxwriter') as writer:
        g_df_sum.to_excel(writer, sheet_name='global')

if __name__ == '__main__':
    # Set up an argument parser
    parser = argparse.ArgumentParser(
        description=''
    )
    parser.add_argument(
        '--account',
        default=os.environ.get('LW_ACCOUNT', None),
        help='The Lacework account to use'
    )
    parser.add_argument(
        '--subaccount',
        default=os.environ.get('LW_SUBACCOUNT', None),
        help='The Lacework sub-account to use'
    )
    parser.add_argument(
        '--api-key',
        dest='api_key',
        default=os.environ.get('LW_API_KEY', None),
        help='The Lacework API key to use'
    )
    parser.add_argument(
        '--api-secret',
        dest='api_secret',
        default=os.environ.get('LW_API_SECRET', None),
        help='The Lacework API secret to use'
    )

    parser.add_argument(
        '-p', '--profile',
        default=os.environ.get('LW_PROFILE', None),
        help='The Lacework CLI profile to use'
    )
    parser.add_argument(
        '--days',
        default=os.environ.get('LOOKBACK_DAYS', 7),
        type=int,
        help='The number of days in which to search for active containers'
    )
    parser.add_argument(
        '--max-threads',
        type=int,
        default=MAX_THREADS,
        help='Maximum number of threads to be used'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        default=os.environ.get('LW_DEBUG', False),
        help='Enable debug logging'
    )
    args = parser.parse_args()


    main(args)
