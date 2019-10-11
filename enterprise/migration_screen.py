#all of these need inputs

#Custom-made libraries
#from aurora_functions import *
from db_generic import *
from tkinter import *
from tkinter import messagebox
from pymysql import connect
import linecache
import psycopg2
import pymssql
import time
from redshift_functions import *
from mssql_functions import *
from dos_util import *

#better to set this in the environment (computer) fixed permanentlyset path=% path %;C:\temp\mysqlsetup\mysql-8.0.17-winx64\bin

#import tkinter as tk
from tkinter import ttk
import os
import sys

def build_redshift_dict():

    redshift_conn_dict = {
        "redshift_host": var_target_host.get()
        ,"redshift_port" : str(var_target_port.get())
        ,"redshift_user" : var_target_username.get()
        ,"redshift_password" : var_target_password.get()
        ,"redshift_database" : var_target_database.get()
        ,"redshift_schema" : var_target_schema.get()
    }

    return(redshift_conn_dict)

def show_entry_fields():
    global var_host
    global var_port
    global var_username
    global var_password
    global var_database
    global var_schema

    global dos_command

    print("SOURCE")
    print(" dbname=" + var_database.get())
    print(" user=" + var_username.get())
    print(" password=*********")
    print(" port=" + str(var_port.get()))
    print(" host=" + var_host.get())
    print(" schema=" + var_schema.get())



    print("TARGET")
    print("dbname=" + var_target_database.get())
    print(" user=" + var_target_username.get())
    print(" password=*********")
    print(" port=" + str(var_target_port.get()))
    print(" host=" + var_target_host.get())


def test_connectivity():

    try:
        connect_to_mssql_param(mssql_server=var_host.get(), mssql_user=var_username.get(),
                               mssql_password=var_password.get(), mssql_db=var_database.get(),
                               mssql_port=var_port.get())
        messagebox.showinfo ('Connectivity test Source','Successfull')
    except:
        messagebox.showerror ('Connectivity test Source','Connection FAILED')



def test_connectivity_target():

    try:
        connect_to_red_param(str(var_target_port.get()), var_target_host.get(),var_target_username.get(),var_password.get(),var_target_database.get())
        print('Connection to redshift:',' sucessfully created', time.ctime())
        messagebox.showinfo ('Connectivity test TARGET','Successfull')
    except:
        print('Connection to redshift', ' ***ERROR***', time.ctime())
        messagebox.showerror ('Connectivity test TARGET','Connection FAILED')

#    return(conn_redshift)

def load_schema__aurora_list():
    global var_host
    global var_port
    global var_username
    global var_password

    conn_aurora = connect(host=var_host.get(), port=var_port.get(), user=var_username.get(),passwd=var_password.get(), db='bwpgmsdefault')
    list_schema_combo = ["<***All Schemas***>"]
    #PENDING CHANGE AURORA TO MSSQL
    # list_schema_db=db_list_all_schemas(conn_aurora)
    # list_schema_combo=list_schema_combo+list_schema_db
    #
    # comboDomains.config(values = list_schema_combo)
    var_database.set("<***All Schemas***>")  # this can be a dropbox

    return(list_schema_db)

def window_update():
    window.update()


def set_status_processing(counter_all,x_db):
    global msg_out_feedback

    msg_out_feedback =  msg_out_feedback + '\n Schema:' + str(counter_all) + ':' + x_db + '(procesing....)'
    ins = '\n' + str(counter_all) + ':' + x_db + '(procesing....)'
    text_result.insert(INSERT, "\n")
    text_result.insert(INSERT, ins)
    var_status_cumulative.set(msg_out_feedback)
    window.update()


def set_status_finished(counter_success, x_db):
    global msg_out_feedback

    msg_out_feedback = msg_out_feedback + '\n' + 'Total of schemas exported successfully:' + str(counter_success)
    var_status_cumulative.set(msg_out_feedback)
    text_result.insert(INSERT, 'Total of schemas exported successfully:' + str(counter_success))
    window.update()


def call_dos_mysqldump():
    picked_database=var_database.get()
    global var_status_cumulative
    global msg_out_feedback
    global list_schema_combo
    global list_schema_db

    msg_out_feedback=''
    counter_success=0

    if picked_database=="<***All Schemas***>":
        list_process = load_schema_list()
    else:
        list_process =[]
        list_process.append(var_database.get())


    for counter_all,x_db in enumerate(list_process):
        set_status_processing(counter_all,x_db)

        rc=call_dos_mysqldump_onedb(p_database=x_db)
        if rc==1:
            counter_success=counter_success+1

    #End of Loop
    set_status_finished(counter_success,x_db)



def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print
    'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)



def call_dos_mysqldump_onedb(p_database):
    global var_status_cumulative
    global var_status_cumulative

    global var_host
    global var_port
    global var_username
    global var_password
    global var_database

    global wg_out_message
    global var_mysql_path
    global var_output_folder
    global msg_out_feedback

    #new variables for target
    global var_target_host
    global var_target_port
    global var_target_username
    global var_target_password
    global var_target_database


    var_database=p_database

    #it should be automatically setnt to var_database comboDomains.get()

    v_full_path_result_file=f'{var_output_folder.get()}baseline_{var_database}.sql'
   #works1008 dos_command=f'mysqldump.exe --host={var_host.get()} --port={var_port.get()} --user={var_username.get()} --password={var_password.get()} --force --databases {var_database} --lock-all-tables=FALSE --result-file={v_full_path_result_file} --log-error=mysql_error_{var_database}.txt --skip-column-statistics --no-data --skip-quote-names --skip-add-drop-table --skip-quote-names --no-data=true --dump-date --routines=false --no-create-db=TRUE'
    dos_command=f'mysqldump.exe --host={var_host.get()} --port={var_port.get()} --user={var_username.get()} --password={var_password.get()} --databases {var_database} --result-file={v_full_path_result_file} --log-error=mysql_error_{var_database}.txt {var_flags.get()}'
    print(dos_command)

    # var_status_oneline.set("Running mysqldump")
    # var_status_oneline.set("")
    wg_out_message.configure(background='grey90')
#   text_result.delete(1.0, INSERT)

    try:
        os.chdir(f'{var_mysql_path.get()}')
        print('Folder for mysql is valid')

        returnvalue=os.system(dos_command)
        vfile = open(f'{var_output_folder.get()}baseline_{var_database}.sql', 'r')
        v_file_contents = vfile.read()
        if 'CREATE TABLE' in v_file_contents:
            v_warning=0
        else:
            v_warning=1
        #var_oneline_cumulative.set(v_file_contents)
        #text_result.insert(INSERT,v_file_contents)

        xlen = len(v_file_contents)
        if v_warning==1 or xlen <= 1200:
            msg_out_feedback=msg_out_feedback + "WARNING:File too short"
            text_result.insert(INSERT, "WARNING:File too short")
            # wg_out_message.configure(background='red2')
            return(0)
        else:
            msg_out_feedback=msg_out_feedback + f'  Export OK:  Results saved to {v_full_path_result_file} '
            text_result.insert(INSERT,f'  Export OK:    Results saved to {v_full_path_result_file}')
            return (1)
            # wg_out_message.configure(background='green2')


    except OSError as err:
        v_err="OS error: {0}".format(err)
        print(v_err)
        msg_out_feedback = msg_out_feedback + v_err
        var_status_cumulative.set(msg_out_feedback)
        return(0)
    except:
        v_err="Unexpected error running Mysqldump.exe:" #+ sys.exc_info()[0]
        msg_out_feedback = msg_out_feedback + v_err
        var_status_cumulative.set(msg_out_feedback)
        raise



#Get data before building the interface


def set_server_default():
    var_host.set("127.0.0.1")
    var_port.set(1505)
    var_username.set("rsalazar")
    var_database.set("QPTM_GC")  # this can be a dropbox
    var_schema.set("dbo")  # this can be a dropbox

    var_target_host.set("bwp-gms-data-dev.cwkyrxg2o0p1.us-east-1.redshift.amazonaws.com")
    var_target_port.set(5439)
    var_target_username.set("svc_integration")
    var_target_password.set("!INtbW3PtXGgMS") #passwords must be provided by user
    var_target_database.set("bwpgmsdev")  # this can be a dropbox
    var_target_schema.set('***Pick One***')
    var_target_input_s3_bucket.set ('s3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift/')


    var_status_oneline.set("Results coming up...")
    var_status_cumulative.set("File not generated yet -Please click CallMysqldump.exe to start")
    var_mysql_path.set("C:\\temp\\mysqlsetup\\mysql-8.0.17-winx64\\bin")
    var_output_folder.set("C:\\temp\\")
    var_flags.set('***BCP FLAGS CAN GO HERE*****')

def set_local_putty_default():
    global var_host
    global var_port
    global var_username
    global var_password
    global var_database
    global var_schema

    #new variables for target
    global var_target_host
    global var_target_port
    global var_target_username
    global var_target_password
    global var_target_database
    global var_status_oneline
    global var_target_schema

    var_host.set("127.0.0.1")
    var_port.set(3311)
    var_username.set("rsalazar")
    var_password.set("9nCAhhuvXE") #passwords must be provided by user
    var_database.set("QPTM_GC")  # this can be a dropbox
    var_schema.set("dbo")  # this can be a dropbox


    var_target_host.set("127.0.0.1")
    var_target_port.set(3310)
    var_target_username.set("svc_integration")
    var_target_password.set("!INtbW3PtXGgMS") #passwords must be provided by user
    var_target_database.set("bwpgmsdev")  # this can be a dropbox
    var_target_schema.set('***Pick One***')
    var_target_input_s3_bucket.set ('s3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift/')

    var_status_oneline.set("Results coming up...")
    var_status_cumulative.set("File not generated yet -Please click CallMysqldump.exe to start")
    var_mysql_path.set("C:\\temp\\mysqlsetup\\mysql-8.0.17-winx64\\bin")
    var_output_folder.set("C:\\temp\\")
    var_flags.set('***BCP FLAGS CAN GO HERE*****')


def create_source_data_frame(window):

    fr_configure_db = Frame(window, bg="grey96")

    # All the labels
    Label(fr_configure_db, text="SOURCE DATABASE (MICROSOFT SQL").grid(row=0, column=0)
    Label(fr_configure_db, text="Host").grid(row=1, column=0, sticky=E)
    Label(fr_configure_db, text="Port").grid(row=2, column=0, sticky=E)
    Label(fr_configure_db, text="Username").grid(row=3, column=0, sticky=E)
    Label(fr_configure_db, text="Password").grid(row=4, column=0, sticky=E)
    Label(fr_configure_db, text="Database").grid(row=5, column=0, sticky=E)
    Label(fr_configure_db, text="Source Schema").grid(row=6, column=0, sticky=E)

    Label(fr_configure_db, text="bcp.exe path").grid(row=7, column=0, sticky=E)
    Label(fr_configure_db, text="JSON Output Folder:").grid(row=8, column=0, sticky=E)

    # All the inputs
    input_port = Entry(fr_configure_db, textvariable=var_port)
    # 2 is a ttk.Combobox
    comboHost = ttk.Combobox(fr_configure_db, values=["x,y"], textvariable=var_host,
                             width=60)
    input_username = Entry(fr_configure_db, textvariable=var_username)
    input_password = Entry(fr_configure_db, textvariable=var_password, show="*")
    # 5 is a ttk.Combobox
    comboDatabase = ttk.Combobox(fr_configure_db,values=['QPTM_GC','QPTM_GS','Gasquest'], textvariable=var_database, width=60)
    comboSchema = ttk.Combobox(fr_configure_db,values=['dbo','pending_read_Schemas',], textvariable=var_schema, width=60)


    input_path_exe = Entry(fr_configure_db, textvariable=var_mysql_path, width=70)
    input_out_folder = Entry(fr_configure_db, textvariable=var_output_folder, width=70)

    comboHost.grid(row=1, column=2, sticky=W)
    input_port.grid(row=2, column=2, sticky=W)
    input_username.grid(row=3, column=2, sticky=W)
    input_password.grid(row=4, column=2, sticky=W)
    comboDatabase.grid(row=5, column=2, sticky=W)  # Database is line$3
    comboSchema.grid(row=6, column=2, sticky=W)  # Database is line$3

    input_path_exe.grid(row=7, column=2, sticky=W)
    input_out_folder.grid(row=8, column=2, sticky=W)

    return (fr_configure_db)

def create_target_data_frame(window):

    fr_configure_db = Frame(window, bg="grey96")

    # All the labels
    Label(fr_configure_db, text="TARGET DATABASE (REDSHIFT)").grid(row=0, column=0)

    Label(fr_configure_db, text="Host").grid(row=1, column=0, sticky=E)
    Label(fr_configure_db, text="Port").grid(row=2, column=0, sticky=E)
    Label(fr_configure_db, text="Database").grid(row=3, column=0, sticky=E)
    Label(fr_configure_db, text="Username").grid(row=4, column=0, sticky=E)
    Label(fr_configure_db, text="Password").grid(row=5, column=0, sticky=E)

    Label(fr_configure_db, text="*Schema will be automatically appended to the end").grid(row=6, column=2, sticky=E)

    Label(fr_configure_db, text=" ").grid(row=7, column=0, sticky=E)
    Label(fr_configure_db, text="S3 PATH").grid(row=8, column=0, sticky=E)

    # All the inputs
    input_port = Entry(fr_configure_db, textvariable=var_target_port)
    comboHost = ttk.Combobox(fr_configure_db, values=["127.0.0.1","bwp-gms-data-dev.cwkyrxg2o0p1.us-east-1.redshift.amazonaws.com"], textvariable=var_target_host,width=60)
    input_username = Entry(fr_configure_db, textvariable=var_target_username)
    input_password = Entry(fr_configure_db, textvariable=var_target_password, show="*")
    input_database = Entry(fr_configure_db, textvariable=var_target_database)
    comboSchemas = ttk.Combobox(fr_configure_db, values=["gasquest","gasquest2019","gmsdl","gmsdw","qptm_gc","qptm_gs"], textvariable=var_target_schema, width=60)
    input_path_s3 = Entry(fr_configure_db, textvariable=var_target_input_s3_bucket, width=70)

    comboHost.grid(row=1, column=2, sticky=W)
    input_port.grid(row=2, column=2, sticky=W)
    input_database.grid(row=3, column=2, sticky=W)
    input_username.grid(row=4, column=2, sticky=W)
    input_password.grid(row=5, column=2, sticky=W)
    comboSchemas.grid(row=6, column=2, sticky=W)
    #row7 is an empty line
    input_path_s3.grid(row=8, column=2, sticky=W)  # Database is line$3

    return (fr_configure_db)


 #     _       _        _                                 _   _
 #    | |     | |      | |                               | | (_)
 #  __| | __ _| |_ __ _| |__   __ _ ___  ___    __ _  ___| |_ _  ___  _ __
 # / _` |/ _` | __/ _` | '_ \ / _` / __|/ _ \  / _` |/ __| __| |/ _ \| '_ \
 #| (_| | (_| | || (_| | |_) | (_| \__ \  __/ | (_| | (__| |_| | (_) | | | |
 # \__,_|\__,_|\__\__,_|_.__/ \__,_|___/\___|  \__,_|\___|\__|_|\___/|_| |_|
 #
 #

def pack_mssql_var():

    mssql_conn_dict ={
        "mssql_server":var_host.get(),
        "mssql_user":var_username.get(),
        "mssql_database":var_database.get(),
        "mssql_port": str(var_port.get()),
        "mssql_password":var_password.get()
        "mssql_schema":var_schema.get()
    }

    return(mssql_conn_dict)

def step1_export():
    print("hello")
    mdic=pack_mssql_var()


    conn_mssql=connect_to_mssql_dic(mdic)
    print('debug:dictionary for bcp is',mssql_conn_dict)

    mssql_bcp_export_schema_json(p_conn_mssql=conn_mssql, p_mssql_conn_dict=mssql_conn_dict,p_source_schema=mdic.mssql_schema, p_conn_redshift=None,p_target_schema=var_target_schema.get(),p_which_tables="all", p_mode="execute")


def step2_compress():
    path_json_files=var_output_folder.get()
    #dos_compress_files(path_json_files)
    try:
        dos_compress_files(path_json_files)
        messagebox.showinfo ('Gzip compression','Gzip files compressed successfully')
    except:
        messagebox.showerror ('Gzip compression','Failure')


def step3_upload():
    print('This is a manuaal step by now, copy all the files to the s3 bucket')

def step4_run_redshift():
    redshift_conn_dict=build_redshift_dict()
    print(redshift_conn_dict)

    #pending work read preferences from interface and sent it along (mode/filter)
    red_run_copy_command_schema(redshift_conn_dict, p_mode='execute', p_filter="pending") #v_filter_lowecase)

def step5_validate():
    mssql_conn_dict=pack_mssql_var()

    conn_mssql=connect_to_mssql_param(mssql_server=var_host.get(), mssql_user=var_username.get(),
                           mssql_password=var_password.get(), mssql_db=var_database.get(),
                           mssql_port=var_port.get())


    valid_main_schema_rowcount_only(conn_src=conn_mssql, mdic, conn_tgt=conn_redshift, p_target_schema=v_schema, p_filter=v_Filter_Case)




#                  _
#                 (_)
#  _ __ ___   __ _ _ _ __
# | '_ ` _ \ / _` | | '_ \
# | | | | | | (_| | | | | |
# |_| |_| |_|\__,_|_|_| |_|
#
#

dos_command = ''
global var_status_cumulative

list_schema_combo = []
list_schema_combo.append("Configure and then load Schemas")
# def clear_message():
#     global var_status_cumulative
#     var_status_cumulative.set('')


window = Tk()
window.configure(background='gray96')
window.geometry("1200x800+50+50")
vmycellsize = "                           "
Label(window, text=vmycellsize).grid(row=0, column=0)
vmycellsize = "                           "
Label(window, text=vmycellsize).grid(row=0, column=1)
vmycellsize = "                           "
Label(window, text=vmycellsize).grid(row=0, column=2)

#Title and Header with Instructions
window.winfo_toplevel().title("PYMYSQLDUMP-Export schema for Aurora MYSQL database")

#These are special tkinter variables, so they have to be called after tk has been initialized

#SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSss
var_host=StringVar()  # Holds a string; default value ''
var_port=IntVar()  # Holds an int; default value 0
var_username=StringVar()  # Holds a string; default value ''
var_password=StringVar()  # Holds a string; default value ''
var_database=StringVar()
var_schema = StringVar()  # Holds a string; default value ''

#TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT
var_target_host = StringVar()  # Holds a string; default value ''
var_target_port = IntVar()  # Holds an int; default value 0
var_target_username = StringVar()  # Holds a string; default value ''
var_target_password = StringVar()  # Holds a string; default value ''
var_target_database = StringVar()  # Holds a string; default value ''
var_target_schema = StringVar()  # Holds a string; default value ''
var_target_input_s3_bucket = StringVar() #'s3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift/')

#OTHER =========================================================================================
var_status_oneline = StringVar()
var_status_cumulative = StringVar()
var_mysql_path = StringVar()
var_output_folder = StringVar()
var_flags = StringVar()


fr_left_source_db = create_source_data_frame(window)
fr_right_source_db = create_target_data_frame(window)
set_local_putty_default()

window.update()


#BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
# FRAME FOR BUTTON
#BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB

fr_button = Frame(window, bg="grey90")

Button(fr_button, text='Set local defaults', command=set_local_putty_default).grid(row=1, column=1, sticky=W,
                                                                                   pady=4)
Button(fr_button, text='Set server defaults', command=set_server_default).grid(row=2, column=1, sticky=W, pady=4)
Label(fr_button, text="empty line  ", foreground="grey96", bg="grey96").grid(row=3, column=0, sticky=W)
Label(fr_button, text="empty line  ", foreground="grey96", bg="grey96").grid(row=4, column=0, sticky=W)

Button(fr_button, text='Step1:  Export using MSSQL bcp', command=step1_export).grid(row=5, column=1, sticky=W,
                                                                                    pady=4)
Button(fr_button, text='Step2:  Compress using gzip', command=step2_compress).grid(row=6, column=1, sticky=W,
                                                                                   pady=4)
Button(fr_button, text='Step3:  Upload to s3', command=step3_upload).grid(row=7, column=1, sticky=W, pady=4)
Button(fr_button, text='Step4:  Run Redshift copy command', command=step4_run_redshift).grid(row=8, column=1,
                                                                                             sticky=W, pady=4)
Label(fr_button, text="empty line  ", foreground="grey96", bg="grey96").grid(row=8, column=0, sticky=W)
Label(fr_button, text="empty line  ", foreground="grey96", bg="grey96").grid(row=9, column=0, sticky=W)
Button(fr_button, text='Show values for variables now', command=show_entry_fields).grid(row=10, column=1)
Button(fr_button, text='Test connectivity source', command=test_connectivity).grid(row=11, column=1)
Button(fr_button, text='Test Connectivity target', command=test_connectivity_target).grid(row=12, column=1)

Button(fr_button, text='Quit', command=window.quit).grid(row=30, column=1)


#=======================================================================================
# FRAME FOR RESULTS
#=======================================================================================

fr_result=Frame(window,bg="gray96") #bg="blue"


#text_result=Text(fr_result) #,background="gray96") #, textvariable=var_status_cumulative) #, text='Status goes here', width=800, relief='sunken',textvariable=var_status_cumulative) #7
text_result=Text(fr_result)
text_result.insert(INSERT,"line1")
#text_result.pack()
wg_out_message=Message(fr_result, text='Initial test', width=1200, relief='sunken',textvariable=var_status_cumulative) #7
textscrollbar = Scrollbar(fr_result)
textscrollbar.config(command=text_result.yview)
text_result.config(yscrollcommand=textscrollbar.set)
#text_result.insert(INSERT,"MYSQL dump flat file preview will be displayed here")


fr_left_source_db.grid(row=1, column=0)
fr_right_source_db.grid(row=1, column=1)

fr_button.grid(row=2, column=0)  # frame#2 (trowards the right)
#Placement

wg_out_message.grid(row=0,column=0 ,columnspan=8,sticky=W) #The columnspan is important because it makes it very wide.
Label(fr_result, text="     ").grid(row=2,column=0,sticky=E)
#text_result.grid(row=3,column=0 ,columnspan=8,sticky=W) #The columnspan is important because it makes it very wide.
sepline_result1=Label(window, text=' ')
sepline_result2=Label(window, text=' ')
sepline_result1.grid(row=3,column=0)
sepline_result2.grid(row=4,column=0)
fr_result.grid(row=6,column=0) #frame#3



window.mainloop()


# except:
#     PrintException()