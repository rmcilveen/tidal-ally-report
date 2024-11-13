#!/usr/bin/env python3

import pyodbc
import os
from datetime import datetime, timedelta
import smtplib
import ally_rpt_config_prod

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

server = 'eagle\\distribution'
database = 'Tradeking'

driver = '{ODBC Driver 17 for SQL Server}'


time_stamp = datetime.today()
file_time_stamp = datetime.today() - timedelta(days=1)
title_header = "Ally Critical Data Feeds - 2:30am CT Update for Process Date" + file_time_stamp.strftime("%Y-%b-%d") 

#Report Flags
tables_pop = "<b>All Critical Tables Have Been Populated</b><br>\n"
extracts_del = "<b>All Critical Extracts Have Been Delivered</b><br>\n"
eob_flag = "<b>End Of Batch Flag Is Available On Eagle\\TradeKing</b><br>\n"

tidal_db_server = "Ozark\\ITTools" 
tidal_db = "Admiral"


if not os.path.exists("C:/py_scripts/Ally_Report/"):
    print("Ab path:", os.path.abspath(os.getcwd()))
    os.makedirs("C:/py_scripts/Ally_Report/")
filename = "C:/py_scripts/Ally_Report/Ally_report_" + time_stamp.strftime("%Y_%b_%d") + ".html"
'''
dir = os.path.abspath(os.getcwd())
path = os.path.join(dir + "/py_scripts/Ally_Report/")

if not os.path.exists(dir + "/py_scripts/Ally_Report/"):
    print("Ab path:", os.path.abspath(os.getcwd()))
    
    os.makedirs(path)
filename = path + "Ally_report_" + time_stamp.strftime("%Y_%b_%d") + ".html"
'''

#SQL Statements for report
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={ally_rpt_config_prod.username};PWD={ally_rpt_config_prod.password}'
conn_str_TIDAL = f'DRIVER={driver};SERVER={tidal_db_server};DATABASE={tidal_db};UID={ally_rpt_config_prod.username};PWD={ally_rpt_config_prod.password}'

initial_date_string = "declare @PD datetime; select @PD = MAX(processdate) from LoadStatus where Firm = '10';"

query_completed = initial_date_string + "select * from LoadStatus where Firm = '10' and LoadStatusID in ('335','326','361','359','323','322','325','312','321','360','315');"

queryEOB = "select top 1 auditlastupdated, processdate, * From tradeking.dbo.loadstatusaudit where status = 'c' and loadstatusid = 315 order by 2 desc"

queryEXT = "select jm.jobmst_name, jr.jobrun_stachgtm, jr.jobrun_status,jr.* from jobrun (nolock) jr join jobmst jm on jr.jobmst_id = jm.jobmst_id where jr.jobmst_id in ( select b.jobmst_id from [dbo].[jobdtl] a (nolock) join [dbo].[jobmst] b (nolock) on b.jobdtl_id = a.jobdtl_id where a.jobdtl_params like '%KRSG%' and b.jobmst_name like '%020 EXT%') and (jobrun_proddt >= getdate() -2 and jobrun_proddt < getdate()) and jobrun_status <> 0 order by jm. jobmst_name,jr.jobmst_id,jobrun_rundt;"


updated_query = "select COUNT(*) as '{}' from {} where Firm=10 and ProcessDate = @PD"
#updated_query_without_date = "select COUNT(*) as '{}' from {} where Firm=10"
updated_query_without_date = "select COUNT(*) from {} where Firm=10"

#List of reported items that will not return a date column
no_date_list = ("SecurityBase", "OvernightBuyingPower","PositionBreakOutStrategyOvernight","ReviewProcessorEventData")

#Initial SQL call
def call_sql_query(query_str):
    with pyodbc.connect(conn_str) as conn:
        
        cursor = conn.cursor()
        #print(initial_date_string + query_str)
        cursor.execute(initial_date_string + query_str)
        columns = cursor.description[0][0]
        results = cursor.fetchone()
        
        return columns, results[0]
#EXT922, EXT 981 and EXT 982 and based of file modified times
def get_mod_time(path_to_file):
    try:
        return datetime.fromtimestamp(os.path.getmtime(path_to_file))
    except Exception as e:
        print("Error: ", e)		
		
# Compilation of Counts from Eagle/distribution/Tradeking database
def count_compile():
    try:
        print(os.getcwd())
        print(filename)
        with open(filename, "w") as textfile:
            #textfile.write("<h1>Ally Critical Data Feeds - End Of Batch Update for: " + time_stamp.strftime("%Y-%b-%d")+"</h1> \n")
            textfile.write("<style>table, th, td {  border: 1px solid black;  border-collapse: collapse; padding: 5px}</style>\n")
            with pyodbc.connect(conn_str) as conn:
                print(no_date_list)
                eob_not_rec = 0
                cursor = conn.cursor()
                cursor.execute(query_completed)
                columns = cursor.description
                results = cursor.fetchall()
        
                headers = ""
       
                textfile.write("<table>\n")
                textfile.write("\t<tr>\n")
                textfile.write("\t\t<th>Table Name</th><th> Status </th><th> Row Count / End Time </th>\n")
                textfile.write("\t</tr>\n")
                if str(len(results)) == 0:
                    print(str(len(results)) + " - Rows; nothing to display")
                    return False
                for row in results:
                    if row[5] is not None:
                        if row[3] not in no_date_list:
                            new_query = updated_query.format(row[3],row[3])
                        else: 
                            new_query = updated_query_without_date.format(row[3],row[3])
                        count_result = call_sql_query(new_query)[0:]                 
                        if str(row[6]) == "C":
                            status = "Completed"
                        elif str(row[6]) == "P":
                            status = "Processing"
                        elif str(row[6]) == "N":
                            status = "Not Started"
                        #print("<td>" + str(row[3]) + "<\td><td>" + str(row[6]) + "<\td><td>" + row[5].strftime("%c"))
                        #textfile.write("<tr><td>" + str(row[3]) + "</td><td> \t" + str(row[6]) + "</td><td> \t" + row[5].strftime("%X %x")+"</td></tr>\n")
                        #textfile.write("<tr><td>" + str(row[3]) + "</td><td> \t" + str(row[6]) + "</td><td> \t" + str(count_result[1]) +"</td></tr>\n")
                        textfile.write("\t<tr>\n\t\t<td>" + str(row[3]) + "</td>\t<td>" + status + "</td>\t<td>" + str(f"{count_result[1]:,}") +"</td>\n\t</tr>\n")
                    elif row[3] == "End of Batch (Penson)":
                        print()
                
                    else:
                        #print(row)
                        #print("<td>" + str(row[3]) + "<td> \t" + str(row[6]) + "<td> \t" + row[7].strftime("%c"))
                        textfile.write("<tr><td>" + str(row[3]) + "</td><td> \t" + str(row[6]) + "</td><td> \t" + row[7].strftime("%X %x")+"</td></tr>\n")
            
                cursor.execute(queryEOB)
                columns = cursor.description
                results2 = cursor.fetchall()
                #print(str(columns[0][0]))
                textfile.write("\t<tr style='font-weight:bold'>\n")  
                for row2 in results2:
                    #hours = (time_stamp - row2[16]) /3600
                    print("Row value: {}, Today {}".format(int(row2[16].strftime("%d")),int(time_stamp.strftime("%d")))) #print("Hours: ",str(hours))
                    #if hours.seconds <= 2.5:
                    if int(row2[16].strftime("%d")) == int(time_stamp.strftime("%d")):
                        textfile.write("\t\t<td>End Of Batch: </td>\t<td>"+ status +"</td>\t<td>" + row2[16].strftime("%X %x")+"</td>\n")
                        eob_not_rec = 1
                    #elif hours.seconds > 2.5:
                    elif int(row2[16].strftime("%d")) < int(time_stamp.strftime("%d")):
                        
                        textfile.write("\t\t<td>End Of Batch: </td>\t<td>Pending</td>\t<td></td>\n")
                        return False, eob_not_rec
                    else:
                        print(row2[16].strftime("%X %x")," Batch timestamp not current, something may have gone wrong")
                        return False, eob_not_rec
                textfile.write("\t</tr>\n")    
                textfile.write("</table>\n")
                
                return True, eob_not_rec
           
    except pyodbc.Error as e:
        print("Error: ", str(e))
        return False
		
#Compilation of the EXT end times from Tidal
def send_compile():
    
    report_list = ["EXT922", "EXT981", "EXT982"]
    
    try:
        with open(filename,"a") as textfile:
            textfile.write("<br>")
            with pyodbc.connect(conn_str_TIDAL) as conn:
                print("Success")
            
                cursor = conn.cursor()
                cursor.execute(queryEXT)
                columns = cursor.description
                results3 = cursor.fetchall()
            
            
                textfile.write("<table>\n")
                textfile.write("<tr>\n")
                textfile.write("<th>Report</th><th> Status </th><th> EndTime </th>\n")
                textfile.write("</tr>\n")
				
                status_tracker = 0
                #Check if results are empty
                if not results3:
                    return False, status_tracker
                
                for row in results3:
                
                    jobname = row[0].split
					
                    complete = [97,98,101,102]
                    status = ""
                    if row[2] in complete:
                        status = "Complete"
                        textfile.write("<tr><td>" + str(row[0][4:10]) + "</td>\t<td>" + status + "</td>\t<td>" + row[1].strftime("%X %x")+" CT </td></tr>\n")
                    else:
                        status = "Pending"
                        textfile.write("<tr><td>" + str(row[0][4:10]) + "</td>\t<td>" + status + "</td>\t<td>" + row[1].strftime("%X %x")+" CT </td></tr>\n")
                        status_tracker = status_tracker + 1
                for rep in report_list:
                    if rep == "EXT922":
                        path = "//d1wrptfsrprd3/reports/Firm10/krsg/"+file_time_stamp.strftime("%Y%m%d")+"/"+rep+"/"+rep+"_KRSG_"+file_time_stamp.strftime("%Y%m%d")+".csv"
                    else:
                        path = "//d1wrptfsrprd3/reports/Firm10/krsg/"+file_time_stamp.strftime("%Y%m%d")+"/"+rep+"/"+rep+"_KRSG.csv"
                    print(path)
                    if os.path.exists(path):
                        textfile.write("<tr><td>"+rep+"</td>\t<td>Complete</td>\t<td>"+ get_mod_time(path).strftime("%X %x")+" CT </td></tr>\n")
                    else:
                        print(path, " does not exist")
                        textfile.write("<tr><td>"+rep+"</td><td>Pending</td><td></td></tr>\n")
                        status_tracker = 1
                '''
                textfile.write("<tr><td>EXT922</td><td></td><td>"+ get_mod_time("Q:/Firm10/ALLW/"+file_time_stamp.strftime("%Y%m%d")+"/EXT922/EXT922_ALLW_"+file_time_stamp.strftime("%Y%m%d")+".csv").strftime("%X %x")+" CT </td></tr>")
                textfile.write("<tr><td>EXT981</td><td></td><td>"+ get_mod_time("Q:/Firm10/ALLW/"+file_time_stamp.strftime("%Y%m%d")+"/EXT981/EXT981_ALLW_"+file_time_stamp.strftime("%Y%m%d")+".csv").strftime("%X %x")+" CT </td></tr>")
                textfile.write("<tr><td>EXT982</td><td></td><td>"+ get_mod_time("Q:/Firm10/ALLW/"+file_time_stamp.strftime("%Y%m%d")+"/EXT982/EXT982_ALLW_"+file_time_stamp.strftime("%Y%m%d")+".csv").strftime("%X %x")+" CT </td></tr>")
                
                textfile.write("</tr>")    
                '''
                textfile.write("</table>\n")
                print(status_tracker)
                return True, status_tracker
                if status_tracker > 0:
                    return False, status_tracker
    except pyodbc.Error as e:
        print("Error: ", str(e))
        return False

        
        
def send_email_via_smtp(recipient, subject, body):
    try:
        message = MIMEMultipart('alternative')
        message['From'] = ally_rpt_config_prod.sender_address
        message['To'] = recipient
        message['Subject'] = subject
        msg = open(filename,"r")
        #print(msg.read())
        message.attach(MIMEText(msg.read(), 'html'))
    
        # SMTP session
        session =    smtplib.SMTP('nonauthrelay.apexclearing.local',25)#smtplib.SMTP('smtp.office365.com', 587)  # use outlook's smtp server and port ## Updated to internal relay server
        #session.starttls()  # enable security
        #session.login(sender_address, sender_pass)  # login with mail_id and password
        text = message.as_string()
        session.sendmail(ally_rpt_config_prod.sender_address, recipient.split(","), text)
        session.quit()
    
    except Exception as e:
        print("Error", e)


def update_flags(one_line, line_no):
	with open(filename, 'r+') as fp:
		lines = fp.readlines()     # lines is list of line, each element '...\n'
		lines.insert(1, one_line)  # you can use any index if you know the line index
		fp.seek(0)                 # file pointer locates at the beginning to write the whole file again
		fp.writelines(lines)       # write whole lists again to the same file

title = False
cc = False
sc = False

cc_TorF, eob_flag = count_compile()
sc_TorF, x = send_compile()

if x > 0:
##    When we do not have all extracts delivered and all tables populated
    update_flags("<b>Files Are Currently Being received\nUpdates To Follow",1)
    sc = False
    title = False
elif x == 0 and sc_TorF is False:
##  When we have all extracts delivered but do not have all tables populated
    update_flags("<b>All Critical Extracts Have Been Delivered<br>\nFiles Are Currently Being Received<br>\nUpdates To Follow</b><br>\n",1)
    sc = False
    title = False
elif sc_TorF is False:
##  When we have all tables populated but do not have all extracts delivered
    update_flags("<b>All Critical Tables Have Been Delivered<br>\nFiles Are Currently Being Received<br>\nUpdates To Follow<br></b>\n",1)
    sc = False
    title = False
elif sc_TorF is True and eob_flag == 0:
    update_flags("<b>All Critical Extracts Have Been Delivered<br>\nAll Critical Tables Have Been Delivered<br>\nFiles Are Currently Being Received<br>\nUpdates To Follow<br></b>\n",1)
    sc = True
    cc = False
    title = True
else:
    update_flags("<b>All Critical Extracts Have Been Delivered<br> All Critical Tables Have Been Delivered<br> End Of Batch Flag Is Available On Eagle\TradeKing</b>\n",1)
    sc = True
    cc = True
    title = True



if cc is False or sc is False:
    email_Subject = "Ally Critical Data Feeds - 2:30am CT Update for Process Date " + file_time_stamp.strftime("%Y-%b-%d")
    update_flags("<h1>"+email_Subject+"</h1>",0)
else:
    email_Subject = "Ally Critical Data Feeds - End Of Batch Update for " + file_time_stamp.strftime("%Y-%b-%d")
    update_flags("<h1>"+email_Subject+"</h1>",1)

#email_Subject = "Ally Critical Data Feeds - End Of Batch Update for " + time_stamp.strftime("%Y-%b-%d")

send_email_via_smtp('tkbatch@invest.ally.com,hdooley@apexfintechsolutions.com,mkulkarni@apexfintechsolutions.com,rmcilveen@apexfintechsolutions.com,arossi@apexfintechsolutions.com,jrooney@apexfintechsolutions.com', email_Subject, "")  
#send_email_via_smtp('hdooley@apexfintechsolutions.com,mkulkarni@apexfintechsolutions.com,rmcilveen@apexfintechsolutions.com,arossi@apexfintechsolutions.com,jrooney@apexfintechsolutions.com,sgilmore@apexfintechsolutions', email_Subject, "")

print("Ally Report Script complete")
