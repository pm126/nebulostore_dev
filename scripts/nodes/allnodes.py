import xmlrpclib
import logging
import argparse
import getpass

log = logging.getLogger("allnodes")

tophosts = ['roti.mimuw.edu.pl', 'prata.mimuw.edu.pl']

def prepare_auth(username):
    api_server = xmlrpclib.ServerProxy('https://www.planet-lab.eu/PLCAPI/', allow_none=True)

    auth = {}
    auth['AuthMethod'] = 'password'
    auth['Username'] = username
    auth['AuthString'] = getpass.getpass()#password

    authorized = api_server.AuthCheck(auth)
    if authorized:
        log.debug('We are authorized!')
        return (auth, api_server)
    else:
        return None # will fail with an exception anyway

def slice_hosts(auth, api_server, slicename):
    """gets all hosts from slicename; needs username and password for authorization
    """
    node_ids = api_server.GetSlices(auth, slicename, ['node_ids'])[0]['node_ids']
    node_hostnames = [node['hostname'] for node in api_server.GetNodes(auth, node_ids, ['hostname'])]
    return node_hostnames

def filter_booted(auth, api_server, hostnames):
    """from hostnames (list of hosts) returns ones that are in boot state
    """
    node_info = api_server.GetNodes(auth, hostnames, ['hostname', 'boot_state'] )
    booted = [ node['hostname'] for node in node_info if node['boot_state'] == 'boot' ]
    return booted

def add_nodes(auth, api_server, slicename, hostnames):
    api_server.AddSliceToNodes(auth, slicename, hostnames)

def remove_node(auth, api_server, slicename, hostnames):
    api_server.DeleteSliceFromNodes(auth, slicename, hostnames)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get the list of (booted) hosts for a planet-lab slice.')
    parser.add_argument('username', help='username on planet-lab.eu website')
    #parser.add_argument('password', help='password for the username')
    parser.add_argument('-s', '--slice', default='mimuw_nebulostore', help='name of the slice (default: mimuw_nebulostore)')

    args = parser.parse_args()
    (auth, api_server) = prepare_auth(args.username)
    #with open('unavailable_hosts.txt') as f:
    #	lines = f.read().splitlines()
    #remove_node(auth, api_erver, args.slice, lines)
    node_hostnames = slice_hosts(auth, api_server, args.slice)
    #add_nodes(auth, api_server, args.slice, node_hostname)
    booted_hostnames = filter_booted(auth, api_server, node_hostnames, )
    for tophost in tophosts:
        if tophost in booted_hostnames:
            print tophost
            booted_hostnames.remove(tophost)
    for hostname in booted_hostnames:
        print hostname
