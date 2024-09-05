import pyodbc
import os
import datetime
import smtplib
import ally_rpt_config_prod

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

server = 'eagle\\distribution'
database = 'Tradeking'
#username = 'TidalAdmin'
#password = 'T1d@lOps'
driver = '{ODBC Driver 17 for SQL Server}'

#server = 'D1WAPESQLNP1\\UAT2'
#database = 'TradeKing'
#username = 'TidalAdmin_UAT'
#password = 'T!d@l@dm!nU@t'
#driver = '{ODBC Driver 11 for SQL Server}'

time_stamp = datetime.datetime.now()

#Report Flags
tables_pop = "<b>All Critical Tables Have Been Populated</b><br>"
extracts_del = "<b>All Critical Extracts Have Been Delivered</b><br>"
eob_flag = "<b>End Of Batch Flag Is Available On Eagle\\TradeKing</b><br>"

tidal_db_server = "Ozark\\ITTools" #"D1WLUOSQLNP1\\UAT" #"Ozark\\ITTools"
tidal_db = "Admiral"
#print(os.getcwd())

if not os.path.exists("C:/py_scripts/Ally_Report/"):
    #print("Ab path:", os.path.abspath(os.getcwd()))
    os.mkdir("C:/py_scripts/Ally_Report/")
filename = "C:/py_scripts/Ally_Report/Ally_report_" + time_stamp.strftime("%Y_%b_%d") + ".html"

#if not os.path.exists("/Users/rmcilveen/Documents/Python_Scripts/Ally_Report/"):
#    print("Ab path:", os.path.abspath(os.getcwd()))
#    os.mkdir("/Users/rmcilveen/Documents/Python_Scripts/Ally_Report/")
#filename = "/Users/rmcilveen/Documents/Python_Scripts/Ally_Report/Ally_report_test_" + time_stamp.strftime("%Y_%b_%d") + ".html"

conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={ally_rpt_config_prod.username};PWD={ally_rpt_config_prod.password}'
conn_str_TIDAL = f'DRIVER={driver};SERVER={tidal_db_server};DATABASE={tidal_db};UID={ally_rpt_config_prod.username};PWD={ally_rpt_config_prod.password}'

initial_date_string = "declare @PD datetime; select @PD = MAX(processdate) from LoadStatus where Firm = '10';"

query_completed = initial_date_string + "select * from LoadStatus where Firm = '10' and LoadStatusID in ('335','326','361','359','323','322','325','312','321','360','315');"

queryEOB = "select top 1 auditlastupdated, processdate, * From tradeking.dbo.loadstatusaudit where status = 'c' and loadstatusid = 315 order by 2 desc"

queryEXT = "select jm.jobmst_name, jr.jobrun_stachgtm, jr.jobrun_status from jobrun (nolock) jr join jobmst jm on jr.jobmst_id = jm.jobdtl_id where jr.jobmst_id in ( 	select b.jobmst_id from [dbo].[jobdtl] a (nolock)  		join [dbo].[jobmst] b (nolock) on b.jobmst_id = a.jobdtl_id 		where a.jobdtl_params like '%KRSG%' and b.jobmst_name like '%Zip EXT%')and (jobrun_proddt >= getdate() -2 and jobrun_proddt < getdate())and jobrun_status <> 0 order by jm. jobmst_name,jr.jobmst_id,jobrun_rundt;"


updated_query = "select COUNT(*) as '{}' from {} where Firm=10 and ProcessDate = @PD"
updated_query_without_date = "select COUNT(*) as '{}' from {} where Firm=10"

no_date_list = ("SecurityBase", "OverNightBuyingPower","PositionBreakoutStrategyOvernight","ReviewProcessorEventData")

def call_sql_query(query_str):
    with pyodbc.connect(conn_str) as conn:
        
        cursor = conn.cursor()
        #print(initial_date_string + query_str)
        cursor.execute(initial_date_string + query_str)
        columns = cursor.description[0][0]
        results = cursor.fetchone()
        
        return columns, results[0]
# Compilation of Counts from Eagle/distribution/Tradeking database
def count_compile():
    try:
        print(os.getcwd())
        print(filename)
        with open(filename, "w") as textfile:
            textfile.write("<h1>Ally Critical Data Feeds - End Of Batch Update for: " + time_stamp.strftime("%Y-%b-%d")+"</h1> \n")
            textfile.write("<style>table, th, td {  border: 1px solid black;  border-collapse: collapse; padding: 5px}</style>\n")
            with pyodbc.connect(conn_str) as conn:
                #print("Success")
        
                cursor = conn.cursor()
                cursor.execute(query_completed)
                columns = cursor.description
                results = cursor.fetchall()
        
                headers = ""
       
                textfile.write("<table>")
                textfile.write("<tr>")
                textfile.write("<th>Table Name</th><th> Status </th><th> EndTime </th>\n")
                textfile.write("</tr>")
                if str(len(results)) == 0:
                    print(str(len(results)) + " - Rows; nothing to display")
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
                        #print("<td>" + str(row[3]) + "<\td><td> \t" + str(row[6]) + "<\td><td> \t" + row[5].strftime("%c"))
                        #textfile.write("<tr><td>" + str(row[3]) + "</td><td> \t" + str(row[6]) + "</td><td> \t" + row[5].strftime("%X %x")+"</td></tr>\n")
                        #textfile.write("<tr><td>" + str(row[3]) + "</td><td> \t" + str(row[6]) + "</td><td> \t" + str(count_result[1]) +"</td></tr>\n")
                        textfile.write("<tr><td>" + str(row[3]) + "</td><td> \t" + status + "</td><td> \t" + str(f"{count_result[1]:,}") +"</td></tr>\n")
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
                textfile.write("<tr>")  
                for row2 in results2:
                    #print("End Of Batch: " + row2[16].strftime("%c"))
                    textfile.write("<td>End Of Batch: </td><td>"+ status +"</td><td>" + row2[16].strftime("%X %x")+"</td>\n")
                textfile.write("</tr>")    
                textfile.write("</table>")
                

           
    except pyodbc.Error as e:
        print("Error: ", str(e))
        return False
		
#Compilation of the EXT end times from Tidal
def send_compile():
    try:
        with open(filename,"a") as textfile:
            textfile.write("<br>")
            with pyodbc.connect(conn_str_TIDAL) as conn:
                print("Success")
            
                cursor = conn.cursor()
                cursor.execute(queryEXT)
                columns = cursor.description
                results3 = cursor.fetchall()
            
            
                textfile.write("<table>")
                textfile.write("<tr>")
                textfile.write("<th>Report</th><th> Status </th><th> EndTime </th>\n")
                textfile.write("</tr>")
                for row in results3:
                
                    jobname = row[0].split
                    complete = [97,98,101,102]
                    status = ""
                    if row[2] in complete:
                        status = "Complete"
                        textfile.write("<tr><td>" + str(row[0][8:]) + "\t</td><td> \t" + status + "</td><td> \t" + row[1].strftime("%X %x")+" CT </td></tr>\n")
                    else:
                        status = "Pending"
                        textfile.write("<tr><td>" + str(row[0][8:]) + "\t</td><td> \t" + status + "</td><td> \t" + row[1].strftime("%X %x")+" CT </td></tr>\n")
                textfile.write("</tr>")    
                textfile.write("</table>")
    except pyodbc.Error as e:
        print("Error: ", str(e))
        return False

        
        
def send_email_via_smtp(recipient, subject, body):
    try:
        #sender_address = 'tidal-uat@apexclearing.com'
        #sender_pass = '!i8Cs%NHNqwl'
        message = MIMEMultipart('alternative')
        message['From'] = ally_rpt_config_prod.sender_address
        message['To'] = recipient
        message['Subject'] = subject
        msg = open(filename,"r")
        #print(msg.read())
        message.attach(MIMEText(msg.read(), 'html'))
    
        # SMTP session
        session =    smtplib.SMTP('nonauthrelay.apexclearing.local',25)#smtplib.SMTP('smtp.office365.com', 587)  # use outlook's smtp server and port
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
		lines.insert(0, one_line)  # you can use any index if you know the line index
		fp.seek(0)                 # file pointer locates at the beginning to write the whole file again
		fp.writelines(lines)       # write whole lists again to the same file
    
    
if count_compile() is False :
    update_flags("<b>Table data unavailable</b><br>",0)
    update_flags("<b>End of Batch data unavailable</b><br>",1)
else: 
    update_flags(tables_pop,0)
    update_flags(eob_flag,1)
	
if send_compile() is False:
    update_flags("<b>Extract data unavailable</b><br>",2)
else:
    update_flags(extracts_del,1)
#update_flags(tables_pop,0)
#update_flags(extracts_del,1)
#update_flags(eob_flag,2)

email_Subject = "Ally Critical Data Feeds - End Of Batch Update for " + time_stamp.strftime("%Y-%b-%d")
send_email_via_smtp('hdooley@apexfintechsolutions.com,mkulkarni@apexfintechsolutions.com,rmcilveen@apexfintechsolutions.com,arossi@apexfintechsolutions.com,jrooney@apexfintechsolutions.com', email_Subject, "")  
print("Ally Report Script complete")