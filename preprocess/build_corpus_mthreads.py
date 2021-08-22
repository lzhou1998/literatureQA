import json
from tqdm import tqdm
import pymysql
import threading


def fetch_by_part(cursor, statement, remove_tuple=False, fetch_num=100000):
    res = []
    cursor.execute(statement)
    while True:
        res_part = cursor.fetchmany(fetch_num)
        if len(res_part) == 0:
            break
        if remove_tuple:
            for col in res_part:
                res.append(col[0])
        else:
            res += res_part
    return res


def worker(cs_papers_id, cs_fields_id, cs_aavs_id, file_id):
    db = pymysql.connect(
        host="202.120.36.29",
        port=13306,
        user="readonly",
        passwd="readonly",
        db="am_paper"
    )
    cursor = db.cursor()
    lines = []
    for idx, paper_id in enumerate(tqdm(cs_papers_id)):

        text_tail = " sepsepsep [unused1] sepsepsep "
        annotation = "[_end_][unused1][_map_]" + "Q" + str(paper_id)
        text_raw = fetch_by_part(cursor=cursor, 
                                statement="SELECT abstract FROM am_paper_abstract WHERE paper_id = '{}'".format(str(paper_id)), 
                                remove_tuple=True)
        if len(text_raw) == 0:
            continue
        else:
            text = text_raw[0]
            text = text.replace("\n", " ").strip()
            text = text.replace("\r", " ").strip()
            if len(text) == 0:
                continue
        author_id_raw = fetch_by_part(cursor=cursor, 
                                    statement="SELECT author_id FROM am_paper_author WHERE paper_id = '{}' AND sequence = '1'".format(str(paper_id)), 
                                    remove_tuple=True)
        if len(author_id_raw) > 0:
            author_id = author_id_raw[0]
        else:
            author_id = 0
        if author_id > 0 and author_id in cs_aavs_id:
            text_tail += " sepsepsep [unused2] sepsepsep "
            annotation += "[_end_][unused2][_map_]" + "Q" + str(author_id)

            aff_id_raw = fetch_by_part(cursor=cursor, 
                                statement="SELECT last_known_affiliation_id FROM am_author WHERE author_id = '{}'".format(str(author_id)), 
                                remove_tuple=True)
            if len(aff_id_raw) > 0:
                aff_id = aff_id_raw[0]
            else:
                aff_id = 0
            if aff_id > 0 and aff_id in cs_aavs_id:
                text_tail += " sepsepsep [unused3] sepsepsep "
                annotation += "[_end_][unused3][_map_]" + "Q" + str(aff_id)
        venue_id_raw = fetch_by_part(cursor=cursor, 
                                        statement="SELECT conference_instance_id, journal_id FROM am_paper WHERE paper_id = '{}'".format(str(paper_id)), 
                                        remove_tuple=False)
        if len(venue_id_raw) > 0:
            venue_id = max(venue_id_raw[0])
        else:
            venue_id = 0
        if venue_id > 0 and venue_id in cs_aavs_id:
            text_tail += " sepsepsep [unused4] sepsepsep "
            annotation += "[_end_][unused4][_map_]" + "Q" + str(venue_id)
        fields_id_selected = list()
        fields_id_raw = fetch_by_part(cursor=cursor, 
                                        statement="SELECT field_id, score FROM am_paper_field WHERE paper_id = '{}' AND score >= '0'".format(str(paper_id)), 
                                        remove_tuple=False)
        for field_id_raw, field_score in fields_id_raw:
            if field_id_raw in cs_fields_id:
                fields_id_selected.append((field_id_raw, field_score))
        if len(fields_id_selected) > 4:
            fields_id_selected.sort(key=lambda x: x[1], reverse=True)
            fields_id_selected = fields_id_selected[0: 4]
        fields_id_selected = [x[0] for x in fields_id_selected]
        for field_id in fields_id_selected:
            text_tail += " sepsepsep [unused5] sepsepsep "
            annotation += "[_end_][unused5][_map_]" + "Q" + str(field_id)
        line = text + text_tail + annotation
        lines.append(line)
    with open("corpus/data_{}".format(file_id), "w") as f:
        for l in lines:
            f.write(l + str("\n"))
    print("Saved corpus/data_{}".format(str(file_id)))


def main():
    # Connecting database
    db = pymysql.connect(
        host="202.120.36.29",
        port=13306,
        user="readonly",
        passwd="readonly",
        db="am_paper"
    )
    cursor = db.cursor()

    # Loading entity id
    with open("acekg/cs_papers_id.json", "r") as f:
        cs_papers_id = list(json.loads(f.read()))
    
    with open("acekg/cs_fields_id.json", "r") as f:
        cs_fields_id = set(json.loads(f.read()))
    
    with open("acekg/cs_aavs_id.json", "r") as f:
        cs_aavs_id = set(json.loads(f.read()))

    parts = 1000

    part_length = len(cs_papers_id) // parts

    cs_papers_id_parts = [cs_papers_id[i * part_length: (i + 1) * part_length] for i in range(parts)]

    threads_pool = [threading.Thread(target=worker, args=(cs_papers_id_parts[i], cs_fields_id, cs_aavs_id, i)) for i in range(parts)]
    threads_pool.append(threading.Thread(target=worker, args=(cs_papers_id[parts * part_length: ], cs_fields_id, cs_aavs_id, parts)))

    for t in threads_pool:
        t.start()
    for t in threads_pool:
        t.join()

if __name__ == "__main__":
    main()
