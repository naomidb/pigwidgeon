docstr = """
Pigwidgeon
Usage:
    pigwidgeon.py (-h | --help)
    pigwidgeon.py (-a | -r) [-f] <config_file>

Options:
 -h --help        Show this message and exit
 -a --api         Use VIVO api to upload data immediately
 -r --rdf         Produce rdf files with data
 -f --file        Use XML file to generate PMID list
 """

from docopt import docopt
import os
import os.path
import sys
import datetime
import xml.etree.ElementTree as ET
import yaml

from vivo_utils.handlers.pubmed_handler import Citation, PHandler
from vivo_utils import queries
from vivo_utils.connections.vivo_connect import Connection
from vivo_utils.triple_handler import TripleHandler
from vivo_utils.update_log import UpdateLog
from vivo_utils import vivo_log

CONFIG_PATH = '<config_file>'
_api = '--api'
_rdf = '--rdf'
_file = '--file'

def get_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = yaml.load(config_file.read())
    except:
        print("Error: Check config file")
        exit()
    return config

def make_folders(top_folder, sub_folders=None):
    if not os.path.isdir(top_folder):
        os.mkdir(top_folder)

    if sub_folders:
        sub_top_folder = os.path.join(top_folder, sub_folders[0])
        top_folder = make_folders(sub_top_folder, sub_folders[1:])

    return top_folder

def identify_author(connection, tripler, ulog, db_name):
    author_n = input("Enter the n number of the person (if you do not know, leave blank): ")
    if author_n:
        return author_n
    else:
        print("Enter the person's name.")
        first_name = input("First name: ")
        middle_name = input("Middle name: ")
        last_name = input("Last name: ")
        if last_name:
            full_name = last_name
        else:
            print("You must enter a last name.")
            last_name = input("Last name: ")
            full_name = last_name
        if first_name or middle_name:
            full_name += ", "
            if first_name:
                full_name += first_name
                if middle_name:
                    full_name += (" " + middle_name)
            elif middle_name:
                full_name += middle_name

        matches = vivo_log.lookup(db_name, 'authors', full_name, 'display')
        if len(matches)==0:
            matches = vivo_log.lookup(db_name, 'authors', full_name, 'display', True)

        if len(matches)==1:
            author_n = matches[0][0]
        elif len(matches)>1:
            choices = {}
            count = 1
            for row in matches:
                if (connection.namespace + row[0]) not in choices.values():
                    choices[count] = connection.namespace + row[0]
                    count += 1
            index = -1
            for key, val in choices.items():
                print(str(key) + ': ' + val + '\n')
            index = input("Do any of these match your input? (if none, write -1): ")
            if not index == -1:
                nnum = choices[int(index)]
                author_n = nnum.split(connection.namespace)[-1]
            else:
                matches = []

        if len(matches)==0:
            create_obj = input("This person does not exist in VIVO or could not be found. Would you like to add them? (y/n) ")
            if create_obj == 'y' or create_obj == 'Y':
                print("Fill in the following details. Leave blank if you do not know what to write.")
                params = queries.make_person.get_params(connection)
                author = params['Author']
                author.first = first_name
                author.middle = middle_name
                author.last = last_name
                author.name = full_name
                details = author.get_details()
                for detail in details:
                    item_info = input(str(detail) + ": ")
                    setattr(author, detail, item_info)

                result = tripler.update(queries.make_person, **params)
                ulog.add_to_log('authors', author.name, (connection.namespace + params['Author'].n_number))
                print('*' * 6 + '\nAdding person\n' + '*' * 6)
                author_n = params['Author'].n_number
            else:
                exit()

    return author_n

def get_premade_list(pmid_file):
    tree = ET.parse(pmid_file)
    root = tree.getroot()

    pmids = []
    for citing in root.iter('Item'):
        pmid = ''
        for ident in citing.find('Identifiers').iter():
            try:
                if ident.attrib['name'] == 'PMID':
                    pmid = ident.text
            except KeyError:
                pass
        if pmid:
            pmids.append(pmid)
        else:
            print('No PMID: ' + citing.find('Title').text)

    return pmids

def check_filter(abbrev_filter, name_filter, name):
    cleanfig = get_config(abbrev_filter)
    abbrev_table = cleanfig.get('abbrev_table')
    name += " " #Add trailing space
    name = name.replace('\\', '')
    for abbrev in abbrev_table:
        if (abbrev) in name:
            name = name.replace(abbrev, abbrev_table[abbrev])
    name = name[:-1] #Remove final space

    namefig = get_config(name_filter)
    try:
        if name.upper() in namefig.keys():
            name = namefig.get(name.upper())
    except AttributeError as e:
        name = name

    return name

def process(connection, publication, author, tripler, ulog, db_name, filter_folder):
    abbrev_filter = os.path.join(filter_folder, 'general_filter.yaml')
    j_filter = os.path.join(filter_folder, 'journal_filter.yaml')
    journal_n = None
    if publication.journal:
        publication.journal = check_filter(abbrev_filter, j_filter, publication.journal)
        journal_matches = vivo_log.lookup(db_name, 'journals', publication.journal, 'name')
        if len(journal_matches) == 0:
            journal_matches = vivo_log.lookup(db_name, 'journals', publication.journal, 'name', True)
            if len(journal_matches) == 0:
                journal_matches = vivo_log.lookup(db_name, 'journals', publication.issn, 'issn')
        if len(journal_matches) == 1:
            journal_n = journal_matches[0][0]
        else:
            journal_params = queries.make_journal.get_params(connection)
            journal_params['Journal'].name = publication.journal
            journal_params['Journal'].issn = publication.issn
            tripler.update(queries.make_journal, **journal_params)

            journal_n = journal_params['Journal'].n_number
            ulog.add_to_log('journals', publication.journal, (connection.namespace + journal_n))
            if len(journal_matches) > 1:
                jrn_n_list = [journal_n]
                for jrn_match in journal_matches:
                    jrn_n_list.append(jrn_match[0])
                ulog.track_ambiguities(publication.journal, jrn_n_list)

    pub_n = add_pub(connection, publication, journal_n, tripler, ulog, db_name)

    if pub_n:
        a_params = queries.add_author_to_pub.get_params(connection)
        a_params['Article'].n_number = pub_n
        a_params['Author'].n_number = author

        added = queries.check_author_on_pub.run(connection, **a_params)
        if not added:
            tripler.update(queries.add_author_to_pub, **a_params)

def add_pub(connection, publication, journal_n, tripler, ulog, db_name):
    pub_type = None
    query_type = None

    if 'Journal Article' in publication.types:
        pub_type = 'academic_article'
        query_type = getattr(queries, 'make_academic_article')
    elif 'Editorial' in publication.types:
        pub_type = 'editorial'
        query_type = getattr(queries, 'make_editorial_article')
    elif 'Letter' in publication.types:
        pub_type = 'letter'
        query_type = getattr(queries, 'make_letter')
    elif 'Abstract' in publication.types:
        pub_type = 'abstract'
        query_type = getattr(queries, 'make_abstract')
    else:
        query_type = 'pass'

    publication_matches = vivo_log.lookup(db_name, 'publications', publication.title, 'title')
    if len(publication_matches) == 0:
        publication_matches = vivo_log.lookup(db_name, 'publications', publication.doi, 'doi')
    if len(publication_matches) == 1:
        pub_n = publication_matches[0][0]
    else:
        pub_params = queries.make_academic_article.get_params(connection)
        pub_params['Journal'].n_number = journal_n
        pub_params['Article'].name = publication.title
        pub_params['Article'].volume = publication.volume
        pub_params['Article'].issue = publication.issue
        pub_params['Article'].publication_year = publication.year
        pub_params['Article'].doi = publication.doi
        pub_params['Article'].pmid = publication.pmid
        pub_params['Article'].start_page = publication.start_page
        pub_params['Article'].end_page = publication.end_page
        pub_params['Article'].number = publication.number

        if query_type=='pass':
            ulog.track_skips(publication.pmid, publication.type, **pub_params)
            pub_n = None
        else:
            tripler.update(query_type, **pub_params)
            pub_n = pub_params['Article'].n_number
            ulog.add_to_log('articles', publication.title, (connection.namespace + pub_n))

        if len(publication_matches) > 1:
            pub_n_list = []
            for pub_match in publication_matches:
                pub_n_list.append(pub_match[0])
            if pub_n:
                pub_n_list.append(pub_n)
            ulog.track_ambiguities(publication.title, pub_n_list)
    return pub_n

def main(args):
    config = get_config(args[CONFIG_PATH])
    email = config.get('email')
    password = config.get ('password')
    update_endpoint = config.get('update_endpoint')
    query_endpoint = config.get('query_endpoint')
    namespace = config.get('namespace')
    filter_folder = config.get('filter_folder')

    db_name = '/tmp/vivo_temp_storage.db'

    connection = Connection(namespace, email, password, update_endpoint, query_endpoint)
    handler = PHandler(email)
    vivo_log.update_db(connection, db_name, ['authors', 'journals', 'publications'])
        
    try:
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y_%m_%d_%H_%M")
        full_path = make_folders(config.get('folder_for_logs'), [now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")])

        output_file = os.path.join(full_path, (timestamp + '_pig_output_file.txt'))
        upload_file = os.path.join(full_path, (timestamp + '_pig_upload_log.txt'))
        skips_file = os.path.join(full_path, (timestamp + '_pig_skips_.json'))
        
        tripler = TripleHandler(args[_api], connection, output_file)
        ulog = UpdateLog()
        
        author = identify_author(connection, tripler, ulog, db_name)
        
        if args[_file]:
            q_info = get_premade_list(config.get('input_file'))
        else:
            q_info = input("Write your pubmed query: ")
        results = handler.get_data(q_info, output_file)
        publications = handler.parse_api(results)

        for publication in publications:
            process(connection, publication, author, tripler, ulog, db_name, filter_folder)

        file_made = ulog.create_file(upload_file)
        ulog.write_skips(skips_file)

        if args[_rdf]:
            rdf_file = timestamp + '_upload.rdf'
            rdf_filepath = os.path.join(full_path, rdf_file)
            tripler.print_rdf(rdf_filepath)
            print('Check ' + rdf_filepath)

        os.remove(db_name)
    except Exception as e:
        os.remove(db_name)
        import traceback
        exit(traceback.format_exc())

if __name__ == '__main__':
    args = docopt(docstr)
    main(args)
