import re

def get_OT_ind(action):
    if action['periodType'] == 'OVERTIME':
        return 1
    return 0

def get_home_possesion(action, home_id):
    if action['possession'] == home_id:
        return 1
    return 0

def get_score_diff(action):
    return int(action['scoreHome']) - int(action['scoreAway'])

def get_time_remaining(action):
    time_str = action['clock']
    
    nums = re.findall('(\d+)', time_str)
    m = int(nums[0])*60
    s = int(nums[1])
    
    if action['periodType'] == 'REGULAR':
        return (4 - action['period'])*720 + m + s
    else:
        return m + s