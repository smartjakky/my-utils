import hashlib
from datetime import datetime, date


def get_week_of_month(year, month, day):
    """
    获取指定的某天是某个月中的第几周
    周一作为一周的开始
    """
    end = int(datetime(year, month, day).strftime("%W"))
    begin = int(datetime(year, month, 1).strftime("%W"))
    return end - begin + 1


def get_date(date_string):
    now = datetime.now().date()
    map3 = {0: 'year', 1: 'month', 2: 'day'}
    map4 = {0: 'year', 1: 'month', 2: 'week', 3: 'weekday'}
    m = {3: map3, 4: map4}
    delta_dict = {'year': None, 'month': None, 'day': None, 'week': None, 'weekday': None}
    str_items = []
    for s in date_string.split(' '):
        str_items.extend(s.split('/'))
    if len(str_items) == 3 or len(str_items) == 4:
        for index, item in enumerate(str_items):
            unite = m[len(str_items)][index]
            if '+' in item or '-' in item or item == '0':
                delta_dict[unite] = int(item)
            else:
                if unite == 'week':
                    delta_dict[unite] = int(item) - get_week_of_month(year=now.year, month=now.month, day=now.day)
                elif unite == 'weekday':
                    delta_dict[unite] = int(item) - getattr(now, 'isoweekday')()
                else:
                    delta_dict[unite] = int(item) - getattr(now, unite)
    else:
        raise InvalidDateString
    if delta_dict['week'] is not None:
        delta_dict['day'] = int(delta_dict['week']) * 7 + int(delta_dict['weekday'])
        year = now.year + int(delta_dict['year'])
        month = now.month + int(delta_dict['month'])
        day = now.day + int(delta_dict['week'])*7 + int(delta_dict['weekday'])*1
        d = date(year=year, month=month, day=day)
        # TODO 非法年月日情况
    else:
        year = now.year + int(delta_dict['year'])
        month = now.month + int(delta_dict['month'])
        day = now.day + int(delta_dict['day'])
        d = date(year=year, month=month, day=day)
    return d.strftime('%m/%d/%Y')

if __name__ == '__main__':
    d1 = get_date('2015/+1/-2')
    d2 = get_date('0/0/1/7')
    d3 = get_date('0/0/1')
    d4 = get_date('0/0/0/1')
    print(d1, d2, d3, d4)
