import json
from tqdm import tqdm
import pymysql

IS_LARGE = False


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

def get_cs_fields_and_relations(cursor):
    # ID of 'Computer science': 2030591755
    init_entities = [2030591755]
    res_entities = set()
    res_relations = set()
    while len(init_entities) > 0:
        current_entity = init_entities.pop(0)
        res_entities.add(current_entity)
        sub_entities = fetch_by_part(cursor=cursor, 
                                     statement="SELECT field_id FROM am_field_relation WHERE parent_id = '{}'".format(str(current_entity)), 
                                     remove_tuple=True)
        for sub_entity in sub_entities:
            res_relations.add((sub_entity, current_entity, 6))
            if sub_entity not in res_entities:
                init_entities.append(sub_entity)
        if len(res_entities) % 100 == 0:
            print("Current num of cs fields:", len(res_entities))
    print("Total num of cs fields:", len(res_entities))
    return list(res_entities), list(res_relations)

def get_onehop_cs_papers(cursor, init_entities):
    res_entities = set()
    while len(init_entities) > 0:
        current_entity = init_entities.pop(0)
        res_entities.add(current_entity)
        if len(res_entities) % 1000 == 0:
            print("Current num of cs papers:", len(res_entities))
        sub_entities = fetch_by_part(cursor=cursor, 
                                     statement="SELECT reference_id FROM am_paper_reference WHERE paper_id = '{}'".format(str(current_entity)), 
                                     remove_tuple=True)
        sub_entities += fetch_by_part(cursor=cursor, 
                                      statement="SELECT paper_id FROM am_paper_reference WHERE reference_id = '{}'".format(str(current_entity)), 
                                      remove_tuple=True)
        for sub_entity in sub_entities:
            res_entities.add(sub_entity)
            if len(res_entities) % 1000 == 0:
                print("Current num of cs papers:", len(res_entities))
    print("Total num of cs papers:", len(res_entities))
    return list(res_entities)


def get_multihop_cs_papers(cursor, init_entities):
    checked_entities = set()
    res_entities = set()
    while len(init_entities) > 0:
        current_entity = init_entities.pop(0)
        if current_entity in checked_entities:
            continue
        res_entities.add(current_entity)
        checked_entities.add(current_entity)
        if len(res_entities) % 1000 == 0:
            print("Current num of cs papers:", len(res_entities))
        sub_entities = fetch_by_part(cursor=cursor, 
                                     statement="SELECT reference_id FROM am_paper_reference WHERE paper_id = '{}'".format(str(current_entity)), 
                                     remove_tuple=True)
        sub_entities += fetch_by_part(cursor=cursor, 
                                      statement="SELECT paper_id FROM am_paper_reference WHERE reference_id = '{}'".format(str(current_entity)), 
                                      remove_tuple=True)
        for sub_entity in sub_entities:
            res_entities.add(sub_entity)
            if sub_entity not in checked_entities:
                init_entities.append(sub_entity)
            if len(res_entities) % 1000 == 0:
                print("Current num of cs papers:", len(res_entities))
        if len(res_entities) >= 10000000:
            break
    print("Total num of cs papers:", len(res_entities))
    return list(res_entities)


def get_aavs_and_relations(cursor, init_entities):
    # aav = author + affiliation + venue
    res_entities = set()
    res_relations = set()
    for current_entity in tqdm(init_entities):
        # First about author and affiliation
        sub_entities = fetch_by_part(cursor=cursor, 
                                     statement="SELECT author_id FROM am_paper_author WHERE paper_id = '{}' AND sequence = '1'".format(str(current_entity)), 
                                     remove_tuple=True)
        if len(sub_entities) > 0:
            for sub_entity in sub_entities:
                sub_sub_entity = fetch_by_part(cursor=cursor, 
                                              statement="SELECT last_known_affiliation_id FROM am_author WHERE author_id = '{}'".format(str(sub_entity)), 
                                              remove_tuple=True)[0]
                res_entities.add(sub_entity)
                res_relations.add((current_entity, sub_entity, 3))
                if sub_sub_entity > 0:
                    res_entities.add(sub_sub_entity)
                    res_relations.add((current_entity, sub_sub_entity, 4))
        # Second about venue
        sub_entities = fetch_by_part(cursor=cursor, 
                                     statement="SELECT conference_instance_id, journal_id FROM am_paper WHERE paper_id = '{}'".format(str(current_entity)), 
                                     remove_tuple=False)
        if len(sub_entities) > 0:
            sub_entity = max(sub_entities[0])
            if sub_entity > 0:
                res_entities.add(sub_entity)
                res_relations.add((current_entity, sub_entity, 2))
    return list(res_entities), list(res_relations)


def get_paper_cite_relations(cursor, init_entities):
    res_relations = set()
    entity_scope = set(init_entities)
    for current_entity in tqdm(init_entities):
        sub_entities = fetch_by_part(cursor=cursor, 
                                     statement="SELECT reference_id FROM am_paper_reference WHERE paper_id = '{}'".format(str(current_entity)), 
                                     remove_tuple=True)
        for sub_entity in sub_entities:
            if sub_entity in entity_scope:
                res_relations.add((current_entity, sub_entity, 1))
    return list(res_relations)
        

def get_paper_field_relations(cursor, init_papers, init_fields):
    res_relations = set()
    field_scope = set(init_fields)
    for current_entity in tqdm(init_papers):
        sub_entities = fetch_by_part(cursor=cursor, 
                                     statement="SELECT field_id FROM am_paper_field WHERE paper_id = '{}' AND score >= '0'".format(str(current_entity)), 
                                     remove_tuple=True)
        for sub_entity in sub_entities:
            if sub_entity in field_scope:
                res_relations.add((current_entity, sub_entity, 5))
    return list(res_relations)



def main():
    # Script for Sampling knowledge graph
    # Entity type: paper, author, field, venue, affiliation
    # Relation type: 1 paper-paper(cite), 2 paper-venue(publish_on), 3 paper-author(written_by), 
    #                4 paper-affiliation(publish_by), 5 paper-field(is_in), 6 field-field(is_subfield_of)

    # Connecting database
    db = pymysql.connect(
        host="202.120.36.29",
        port=13306,
        user="readonly",
        passwd="readonly",
        db="am_paper"
    )
    cursor = db.cursor()
    
    # STEP 1: Selecting cs fields and their relations from AceKG
    try:
        with open("acekg/cs_fields_id.json", "r") as f:
            cs_fields_id = json.loads(f.read())
        with open("acekg/cs_fields_relations.json", "r") as f:
            cs_fields_relations = json.loads(f.read())
    except FileNotFoundError:
        cs_fields_id, cs_fields_relations = get_cs_fields_and_relations(cursor)
        with open("acekg/cs_fields_id.json", "w") as f:
            json.dump(cs_fields_id, f, indent=4)
        with open("acekg/cs_fields_relations.json", "w") as f:
            json.dump(cs_fields_relations, f, indent=4)

    # STEP 2: Get LiteratureQA papers and one-hop reference papers
    try:
        with open("acekg/cs_papers_id.json", "r") as f:
            cs_papers_id = json.loads(f.read())
    except FileNotFoundError:
        with open("../code/dataset/train.json", "r", encoding='utf-8') as f:
            paperQA_dataset = json.loads(f.read())
        with open("../code/dataset/test.json", "r", encoding='utf-8') as f:
            paperQA_dataset += json.loads(f.read())
        init_papers = [item["am_id"] for item in paperQA_dataset]
        if not IS_LARGE:
            cs_papers_id = get_onehop_cs_papers(cursor, init_papers)
        else:
            cs_papers_id = get_multihop_cs_papers(cursor, init_papers)
        with open("acekg/cs_papers_id.json", "w") as f:
            json.dump(cs_papers_id, f, indent=4)
    
    # STEP 3: Get venue, first Author and its affiliation of selected paper, build relation of them
    try:
        with open("acekg/cs_aavs_id.json", "r") as f:
            cs_aavs_id = json.loads(f.read())
        with open("acekg/cs_aavs_relations.json", "r") as f:
            cs_aavs_relations = json.loads(f.read())
    except FileNotFoundError:
        cs_aavs_id, cs_aavs_relations = get_aavs_and_relations(cursor, cs_papers_id)
        with open("acekg/cs_aavs_id.json", "w") as f:
            json.dump(cs_aavs_id, f, indent=4)
        with open("acekg/cs_aavs_relations.json", "w") as f:
            json.dump(cs_aavs_relations, f, indent=4)

    # STEP 4: Build paper-paper(cite) relations
    try:
        with open("acekg/cs_paper_cite_relations.json", "r") as f:
            cs_paper_cite_relations = json.loads(f.read())
    except FileNotFoundError:
        cs_paper_cite_relations = get_paper_cite_relations(cursor, cs_papers_id)
        with open("acekg/cs_paper_cite_relations.json", "w") as f:
            json.dump(cs_paper_cite_relations, f, indent=4)

    # STEP 5: Build paper-field(is_in) relations
    try:
        with open("acekg/cs_paper_field_relations.json", "r") as f:
            cs_paper_field_relations = json.loads(f.read())
    except FileNotFoundError:
        cs_paper_field_relations = get_paper_field_relations(cursor, cs_papers_id, cs_fields_id)
        with open("acekg/cs_paper_field_relations.json", "w") as f:
            json.dump(cs_paper_field_relations, f, indent=4)
    
    # STEP 6: Save KG as entity2id.txt, relation2id.txt, train2id.txt
    try:
        with open("acekg/entity2id.txt", "r") as f:
            pass            
        with open("acekg/relation2id.txt", "r") as f:
            pass
        with open("acekg/train2id.txt", "r") as f:
            pass
        with open("entity_map.txt", "r") as f:
            pass
        with open("alias_entity.txt", "r") as f:
            pass
    except FileNotFoundError:
        cs_entities = cs_fields_id + cs_papers_id + cs_aavs_id
        cs_relations = [1, 2, 3, 4, 5, 6]
        cs_trains = cs_fields_relations + cs_paper_field_relations + cs_paper_cite_relations + cs_aavs_relations
        entity2id = dict()
        relation2id = dict()
        with open("acekg/entity2id.txt", "w") as f:
            f.write(str(len(cs_entities)) + "\n")
            id_counter = 0
            for cs_entity in cs_entities:
                f.write("Q" + str(cs_entity) + "\t" + str(id_counter) + "\n")
                entity2id[cs_entity] = id_counter
                id_counter += 1
        with open("acekg/relation2id.txt", "w") as f:
            f.write(str(len(cs_relations)) + "\n")
            id_counter = 0
            for cs_relation in cs_relations:
                f.write("Q" + str(cs_relation) + "\t" + str(id_counter) + "\n")
                relation2id[cs_relation] = id_counter
                id_counter += 1
        with open("acekg/train2id.txt", "w") as f:
            f.write(str(len(cs_trains)) + "\n")
            for cs_train in cs_trains:
                f.write(str(entity2id[cs_train[0]]) + " " + str(entity2id[cs_train[1]]) + " " + str(relation2id[cs_train[2]]) + "\n")
        with open("acekg/entity_map.txt", "w") as f:
            for cs_entity in cs_entities:
                f.write("Q" + str(cs_entity) + "\t" + "Q" + str(cs_entity) + "\n")
        with open("acekg/alias_entity.txt", "w") as f:
            for cs_entity in cs_entities:
                f.write("Q" + str(cs_entity) + "\t" + "Q" + str(cs_entity) + "\n")



if __name__ == "__main__":
    main()