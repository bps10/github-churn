

def get_user_info(gh, user):
    user_info = {'login': user}
    this_user = gh.user(user)
    user_info['followers_count'] = this_user.followers_count
    user_info['following_count'] = this_user.following_count
    user_info['bio'] = this_user.bio
    user_info['blog'] = this_user.blog
    user_info['company'] = this_user.company
    user_info['created_at'] = this_user.created_at
    user_info['public_repos_count'] = this_user.public_repos_count
    user_info['public_gists_count'] = this_user.public_gists_count
    user_info['hireable'] = this_user.hireable
    user_info['updated_at'] = this_user.updated_at

    return user_info

def get_batch(gh, df, random_indexes, start_index, existing_users=set(),
              batch_size=5000):
    '''
    '''
    d = {}
    i = start_index
    count = 0
    while count < batch_size:
        if count % 200 == 0:
            print(count)

        user = df.iloc[random_indexes[i]]
        if user.actor in existing_users:
            i += 1
        else:
            try:
                user_info = get_user_info(gh, user.actor)
                d[user.actor] = user_info
                d[user.actor]['event_count'] = user.event_count
                d[user.actor]['last_event'] = user.last_event
                d[user.actor]['first_event'] = user.first_event
                d[user.actor]['time_between_first_last_event'] = user.time_between_first_last_event
                
                count += 1
                i += 1
            except:
                # expected to fail if user has deleted profile, counts against rate limited api calls.
                count += 1
                i += 1
                print(user.actor)
            
    return d, start_index + batch_size




# curl -u "bps10" -i https://api.github.com/users?page=50&per_page=100
def get_user_info_(user):
    '''Depreciated.
    '''
    buffer = BytesIO()
    c = pycurl.Curl()
    c.setopt(c.URL, 'https://api.github.com/users/{0}'.format(user))
    c.setopt(c.WRITEDATA, buffer)
    c.perform()
    c.close()

    body = buffer.getvalue()
    # Body is a byte string.
    # We have to know the encoding in order to print it to a text file
    # such as standard output.
    body.decode('iso-8859-1')

    return dict(json.loads(d))