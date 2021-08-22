import json
from tqdm import tqdm
import pymysql


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


def build_ent_dataset(intput_file, output_file):
    # Connecting database
    db = pymysql.connect(
        host="202.120.36.29",
        port=13306,
        user="readonly",
        passwd="readonly",
        db="am_paper"
    )
    cursor = db.cursor()

    # Loading original paper_qa dataset
    with open(intput_file, "r") as f:
        paper_qa_ori = json.loads(f.read())

    # Loading entity id
    with open("acekg/cs_fields_id.json", "r") as f:
        cs_fields_id = set(json.loads(f.read()))
    
    with open("acekg/cs_aavs_id.json", "r") as f:
        cs_aavs_id = set(json.loads(f.read()))
    
    paper_qa_ent = []
    # Link entity
    for idx, qas_ori in enumerate(tqdm(paper_qa_ori)):
        qas_ent = dict()

        abstract = qas_ori["Abstract"]
        paper_id = qas_ori["am_id"]
        abstract_tail = ""
        annotation = []

        # For paper entity
        abstract_tail += " [unused1]"
        annotation.append(paper_id)

        # For author entity and affiliation entity
        author_id_raw = fetch_by_part(cursor=cursor, 
                                  statement="SELECT author_id FROM am_paper_author WHERE paper_id = '{}' AND sequence = '1'".format(str(paper_id)), 
                                  remove_tuple=True)
        if len(author_id_raw) > 0:
            author_id = author_id_raw[0]
        else:
            author_id = 0
        if author_id > 0 and author_id in cs_aavs_id:
            abstract_tail += " [unused2]"
            annotation.append(author_id)

            aff_id_raw = fetch_by_part(cursor=cursor, 
                                statement="SELECT last_known_affiliation_id FROM am_author WHERE author_id = '{}'".format(str(author_id)), 
                                remove_tuple=True)
            if len(aff_id_raw) > 0:
                aff_id = aff_id_raw[0]
            else:
                aff_id = 0
            if aff_id > 0 and aff_id in cs_aavs_id:
                abstract_tail += " [unused3]"
                annotation.append(aff_id)
        
        # For venue entity
        venue_id_raw = fetch_by_part(cursor=cursor, 
                                     statement="SELECT conference_instance_id, journal_id FROM am_paper WHERE paper_id = '{}'".format(str(paper_id)), 
                                     remove_tuple=False)
        if len(venue_id_raw) > 0:
            venue_id = max(venue_id_raw[0])
        else:
            venue_id = 0
        if venue_id > 0 and venue_id in cs_aavs_id:
            abstract_tail += " [unused4]"
            annotation.append(venue_id)
        
        # For field entity
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
            abstract_tail += " [unused5]"
            annotation.append(field_id)
        abstract = abstract + abstract_tail
        for qa in qas_ori.items():
            if qa[0] != "Abstract" and qa[0] != "am_id":
                qas_ent[qa[0]] = qa[1]
        qas_ent["Abstract"] = abstract
        qas_ent["Annotation"] = annotation
        paper_qa_ent.append(qas_ent)
    with open(output_file, "w") as f:
        json.dump(paper_qa_ent, f, indent=4)


if __name__ == "__main__":
    build_ent_dataset("../raw_dataset/train.json", "../dataset/train.json")
    build_ent_dataset("../raw_dataset/test.json", "../dataset/test.json")
