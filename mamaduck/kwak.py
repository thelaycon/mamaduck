import argparse
import sys
from mamaduck.connectors.csv import main as csv_main
from mamaduck.connectors.psql import main as psql_main
from mamaduck.connectors.sqlite import main as sqlite_main

from mamaduck.sink.to_csv import main as to_csv_main
from mamaduck.sink.to_psql import main as to_psql_main
from mamaduck.sink.to_sqlite import main as to_sqlite_main

from colorama import init, Fore

init(autoreset=True)

def main():
    """Main entry point that routes to the appropriate tool based on user input."""

    print(Fore.YELLOW + """
  __  __       _        __  __       _        ____       _   _     ____     _  __    
U|' \/ '|u U  /"\  u  U|' \/ '|u U  /"\  u   |  _"\   U |"|u| | U /"___|   |"|/ /    
\| |\/| |/  \/ _ \/   \| |\/| |/  \/ _ \/   /| | | |   \| |\| | \| | u     | ' /     
 | |  | |   / ___ \    | |  | |   / ___ \   U| |_| |\   | |_| |  | |/__  U/| . \\u   
 |_|  |_|  /_/   \_\   |_|  |_|  /_/   \_\   |____/ u  <<\___/    \____|   |_|\_\    
<<,-,,-.    \\    >>  <<,-,,-.    \\    >>    |||_    (__) )(    _// \\  ,-,>> \\,-. 
 (./  \.)  (__)  (__)  (./  \.)  (__)  (__)  (__)_)       (__)  (__)(__)  \.)   (_/  

    """)
    parser = argparse.ArgumentParser(description="MamaDuck CLI Tool Launcher")
    
    # Add the option to choose the tool to run
    parser.add_argument(
        '--kwak', 
        type=str, 
        choices=['load_csv', 'load_psql', 'load_sqlite', 'to_csv', 'to_psql', 'to_sqlite'], 
        required=True,
        help="Choose the migration tool: 'load_csv', 'load_psql', 'load_sqlite', 'to_csv', 'to_psql', or 'to_sqlite'."
    )
    
    # Parse the command-line arguments
    args, unknown_args = parser.parse_known_args()

    # Routing to the appropriate CLI tool with the remaining arguments
    if args.kwak == 'load_csv':
        # Forward the unknown arguments to the CSV tool
        print("Launching CSV Migration Tool...")
        sys.argv = [sys.argv[0], *unknown_args]  # Adjust sys.argv to pass the arguments
        csv_main()

    elif args.kwak == 'load_psql':
        # Forward the unknown arguments to the PostgreSQL tool
        print("Launching PostgreSQL Migration Tool...")
        sys.argv = [sys.argv[0], *unknown_args]  # Adjust sys.argv to pass the arguments
        psql_main()

    elif args.kwak == 'load_sqlite':
        # Forward the unknown arguments to the SQLite tool
        print("Launching SQLite Migration Tool...")
        sys.argv = [sys.argv[0], *unknown_args]  # Adjust sys.argv to pass the arguments
        sqlite_main()

    elif args.kwak == 'to_csv':
        # Forward the unknown arguments to the CSV Sink tool
        print("Launching CSV Sink Tool...")
        sys.argv = [sys.argv[0], *unknown_args]  # Adjust sys.argv to pass the arguments
        to_csv_main()

    elif args.kwak == 'to_psql':
        # Forward the unknown arguments to the PostgreSQL Sink tool
        print("Launching PostgreSQL Sink Tool...")
        sys.argv = [sys.argv[0], *unknown_args]  # Adjust sys.argv to pass the arguments
        to_psql_main()

    elif args.kwak == 'to_sqlite':
        # Forward the unknown arguments to the SQLite Sink tool
        print("Launching SQLite Sink Tool...")
        sys.argv = [sys.argv[0], *unknown_args]  # Adjust sys.argv to pass the arguments
        to_sqlite_main()

if __name__ == "__main__":
    main()
