import argparse
import sys
from mamaduck.connectors.csv import main as csv_main
from mamaduck.connectors.psql import main as psql_main
from mamaduck.connectors.sqlite import main as sqlite_main

from mamaduck.sink.to_csv import main as to_csv_main
from mamaduck.sink.to_psql import main as to_psql_main
from mamaduck.sink.to_sqlite import main as to_sqlite_main

from colorama import init, Fore
import logging

init(autoreset=True)

class CustomArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        """Override the default error method to provide a user-friendly message."""
        self.print_help()
        print(f"\n{Fore.RED}Error: {message}\n")
        print(f"{Fore.YELLOW}Hint: Use one of the valid subcommands: "
              f"'load_csv', 'load_psql', 'load_sqlite', 'to_csv', 'to_psql', 'to_sqlite'.")
        sys.exit(2)

def main():
    """Main entry point that routes to the appropriate tool based on user input."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Display welcome banner
    print(Fore.YELLOW + """
  __  __       _        __  __       _        ____       _   _     ____     _  __    
U|' \/ '|u U  /"\  u  U|' \/ '|u U  /"\  u   |  _"\   U |"|u| | U /"___|   |"|/ /    
\| |\/| |/  \/ _ \/   \| |\/| |/  \/ _ \/   /| | | |   \| |\| | \| | u     | ' /     
 | |  | |   / ___ \    | |  | |   / ___ \   U| |_| |\   | |_| |  | |/__  U/| . \\u   
 |_|  |_|  /_/   \_\   |_|  |_|  /_/   \_\   |____/ u  <<\___/    \____|   |_|\_\    
<<,-,,-.    \\    >>  <<,-,,-.    \\    >>    |||_    (__) )(    _// \\  ,-,>> \\,-. 
 (./  \.)  (__)  (__)  (./  \.)  (__)  (__)  (__)_)       (__)  (__)(__)  \.)   (_/  
    """)

    # Use the custom argument parser
    parser = CustomArgumentParser(description="MamaDuck CLI Tool Launcher")
    parser.add_argument(
        'kwak', 
        type=str, 
        choices=['load_csv', 'load_psql', 'load_sqlite', 'to_csv', 'to_psql', 'to_sqlite'], 
        help="Choose the migration tool: 'load_csv', 'load_psql', 'load_sqlite', 'to_csv', 'to_psql', or 'to_sqlite'."
    )
    
    args, unknown_args = parser.parse_known_args()

    # Map tool choices to corresponding functions
    tool_mapping = {
        'load_csv': csv_main,
        'load_psql': psql_main,
        'load_sqlite': sqlite_main,
        'to_csv': to_csv_main,
        'to_psql': to_psql_main,
        'to_sqlite': to_sqlite_main,
    }

    try:
        logging.info(f"Launching {args.kwak.replace('_', ' ').title()} Tool...")
        sys.argv = [sys.argv[0], *unknown_args]
        tool_mapping[args.kwak]()
    except Exception as e:
        logging.error(f"An error occurred while executing the tool: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
