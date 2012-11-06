# Read help on the available command-line options prior to any migration with command
$./nuodb-migration --help

Usage:
nuodb-migration --help=<command>|--list|--config=<file> [<command>]

nuodb-migration
  [--help=[command]]    Prints help contents on the requested command
  [--list]              Lists available migration commands
  [--config=file]       Reads definition of the migration process from the XML file and executes it
  [command]             Executes specified migration command

# Dump data from MySQL example database from table1 & table2
$./nuodb-migration dump \
    --source.driver=com.mysql.jdbc.Driver --source.url=jdbc:mysql://localhost:3306/enron --source.catalog=enron \
    --source.username=root --output.type=csv --output.path=/tmp/test/dump.cat

# Load comma separated values to the corresponding tables in NuoDB from /tmp/example/dump.cat
$./nuodb-migration load --target.url=jdbc:com.nuodb://localhost/test --target.username=dba --target.password=goalie --input.path=/tmp/test/dump.cat

./nuodb-migration load --target.url=jdbc:com.nuodb://localhost/test --target.username=dba --target.password=goalie --input.path=/tmp/test/dump.cat