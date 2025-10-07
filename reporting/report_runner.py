import sys
from reporting.report_generator import api_pull
from reporting.report_generator import sbl100
from reporting.report_generator import atypical_spreadsheet
from reporting.report_generator import atypical_sheet_runner

#To call all the reports exist within this project


def generate_input_from_jared(date :str, inputdir: str, outputdir:str):
    return api_pull.generate_input_from_jared(date, inputdir, outputdir)

def sbl100_report(date :str, inputdir: str, outputdir:str):
    return sbl100.generate_excel_report(date, inputdir,outputdir)

'''
def atypical_spreadsheet_report(date :str, inputdir: str, outputdir:str):
    return atypical_spreadsheet.generate_excel_report(date, inputdir,outputdir)
'''

def atypical_spreadsheet_report(date :str, inputdir: str, outputdir:str):
    return atypical_sheet_runner.generate_excel_report(date, inputdir,outputdir)

def run_report(date :str, report_config: str, inputdir: str, outputdir:str):
    return getattr(sys.modules[__name__], report_config)(date, inputdir, outputdir)