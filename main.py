import argparse
import datetime
import logging
import sys
import tempfile
import time
from reporting import report_runner



""" 
#Project Structrue

legacyReporting/
│
├── main.py
├── utils/
│   ├── __init__.py 
│   ├── some_tools.py
├── reporting/ 
│   ├── __init__.py 
│   ├── report_runner.py
│   ├── report_generator/
│   │   ├── __init__.py  
│   │   ├── report1.py
│   ├── report_template/   (where stores all the testing report templates to read)


Example Run:
python legacyReporting/main.py -r skyridge_excel_report -d 2025/04/30 -o "C:/Users/XiaHanlu/workspace/local_reading/output" -i "C:/Users/XiaHanlu/workspace/local_reading/panagram"


"""

#function for arguments definition
def report_arg():
    parser = argparse.ArgumentParser(description="Run Reports!")
    parser.add_argument( '-o', "--output",type=str,required=False, help="output directory")
    parser.add_argument('-i', '--input',type=str,required=False, help="input directory")
    parser.add_argument('-r', "--report",type=str,required=True,help="which report to run?")
    parser.add_argument('-d', '--date', type=str, required=False, help= "date string: default current day's date as 2019/12/31")
    args = parser.parse_args()
    return args

def main(args):
    # Read in datetime
    date = datetime.datetime.strptime(args.date,"%Y/%m/%d") if args.date else datetime.datetime.today()
    # User must select a report to run!
    if args.report:
        report_config = args.report
    else:
        logging.info("Please check your input args, must choose which report to run")
        sys.exit(10)

    # If no directory will just use temporary dir
    with tempfile.TemporaryDirectory() as temp_dir:
        report_runner.run_report(date, report_config, args.input if args.input else temp_dir, args.output if args.output else temp_dir)


if __name__ == "__main__":
    start = time.time()

    args = report_arg()
    main(args)

    end = time.time()
    elapsed = end - start
    print(f"Execution time: {elapsed:.2f} seconds")


