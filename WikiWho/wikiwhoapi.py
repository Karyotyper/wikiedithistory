#Django_Unchained

import requests
import json
import pandas as pd

def get_revisions():
    article_name = "Godfather"

    # get all the revision id
    url = "https://www.wikiwho.net/en/api/v1.0.0-beta/rev_ids/" + article_name + "/?editor=true&timestamp=true"

    req = requests.get(url)
    content = json.loads(req.content)

    all_revisions = []

    i = 0
    for rev in content['revisions']:
        all_revisions.append(rev['id'])
        i = i + 1
        if i == 5:
            break

    # get revision details
    revision_id_list, time_list, token_id_list, editor_id_list, out_list, ins_list, o_revision_id_list = [], [], [], [], [], \
                                                                                                         [], []
    for single_revision in all_revisions:
        single_revision = str(single_revision)
        revision_url = "https://www.wikiwho.net/en/api/v1.0.0-beta/rev_content/rev_id/" + (single_revision) + \
                       "/?o_rev_id=true&editor=true&token_id=true&out=true&in=true"
        r = requests.get(revision_url)
        cont = json.loads(r.content)

        for data in cont['revisions']:
            base = data[single_revision]
            time = base['time']
            for token in base['tokens']:
                token_id = token['token_id']
                editor_id = token['editor']
                out = token['out']
                ins = token['in']
                o_revision_id = token['o_rev_id']

                revision_id_list.append(single_revision)
                time_list.append(time)
                token_id_list.append(token_id)
                editor_id_list.append(editor_id)
                out_list.append(out)
                ins_list.append(ins)
                o_revision_id_list.append(o_revision_id)

    rows = {'revision_id': revision_id_list,
            'time': time_list,
            'token_id': token_id_list,
            'editor_id': editor_id_list,
            'deleted': out_list,
            'reinserted': ins_list,
            'o_rev_id': revision_id_list}

    data = pd.DataFrame(rows)
    # data.to_csv("Revision_Details.csv")

    return data


def fun(x):
    x = x.replace("[", "")
    x = x.replace(']', '')
    return len(x.split(','))


def main():
    revisions = get_revisions()
    # revisions = pd.read_csv("Revisions.csv")

    revisions['number_of_deletes'] = revisions['deleted'].apply(lambda x: fun(x))
    revisions['number_of_reinserted'] = revisions['reinserted'].apply(lambda x: fun(x))

    deletes_by_author_df = pd.DataFrame(revisions.groupby(['editor_id'], as_index=False)['number_of_deletes'].agg({'number_deleted':'sum'}))
    reinserts_by_author_df = pd.DataFrame(revisions.groupby(['editor_id'], as_index=False)['number_of_reinserted'].agg({'number_reinserted':'sum'}))

    tmp_df = revisions[revisions['revision_id']==revisions['o_rev_id']]
    tmp_df = pd.DataFrame(tmp_df.groupby(['editor_id'], as_index=False)['editor_id'].agg({'number_of_token_created':'count'}))

    dfs = [deletes_by_author_df, reinserts_by_author_df, tmp_df]
    df_final = reduce(lambda left,right: pd.merge(left,right,on='editor_id'), dfs)

    # df_final.to_csv("editor_details.csv")

if __name__=='__main__':
    main()
