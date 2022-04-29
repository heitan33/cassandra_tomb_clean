# coding:utf-8
from cassandra import ConsistencyLevel
# 引入数据
from cassandra.cluster import Cluster
# 引入DCAwareRoundRobinPolicy模块，可用来自定义驱动程序的行为
# from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import commands, os, sys

def cassandra_command_ops(cass_host_list, cassandra_command):
    for cass_host in cass_host_list:
        while True:
            os.environ['cassandra_command'] = str(cassandra_command)
            status, remote_repair = commands.getstatusoutput("ssh root@%s $cassandra_command" %(cass_host))
            print(status, remote_repair)
            if status == 0:
                break

def cassandra_alter_gc(cass_host_list, graphindex_sql, edgestore_sql):
    for host in cass_host_list:
        cluster = Cluster(contact_points = [host], port = 9042)
        session = cluster.connect()
        session.execute(graphindex_sql)
        session.execute(edgestore_sql)
        session.shutdown()
        cluster.shutdown()

def main():
    print(cass_host_list)
    print(filename)
    try:
        cassandra_command = filename + " " + "repair bewg_bewg"
        cassandra_command_ops(cass_host_list, cassandra_command)

        graphindex_sql = "ALTER TABLE bewg_bewg.graphindex WITH gc_grace_seconds = '0';"
        edgestore_sql = "ALTER TABLE bewg_bewg.edgestore WITH gc_grace_seconds = '0';"
        cassandra_alter_gc(cass_host_list, graphindex_sql, edgestore_sql)

        cassandra_command = filename + " " + "compact bewg_bewg"
        cassandra_command_ops(cass_host_list, cassandra_command)

        graphindex_sql = "ALTER TABLE bewg_bewg.graphindex WITH gc_grace_seconds = '86400';"
        edgestore_sql = "ALTER TABLE bewg_bewg.edgestore WITH gc_grace_seconds = '86400';"
        cassandra_alter_gc(cass_host_list, graphindex_sql, edgestore_sql)

    except Exception, e:
        print(e)
        sys.exit(2)


if __name__ == "__main__":
    filename = '/opt/apache-cassandra-3.11.4/bin/nodetool'
    if not os.path.exists(filename):
        print("nodetool not exist!!")
        sys.exit(2)

    cass_host_list = ["20.5.2.40", "20.5.2.41", "20.5.2.42"]
    main()