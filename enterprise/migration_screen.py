#all of these need inputs

#Custom-made libraries
#from aurora_functions import *
from db_generic import *
from tkinter import *
from tkinter import messagebox
from pymysql import connect
import linecache
import psycopg2
from pymssql import *
import time
from redshift_functions import *
from mssql_functions import *
from dos_util import *
from validations import *

#better to set this in the environment (computer) fixed permanentlyset path=% path %;C:\temp\mysqlsetup\mysql-8.0.17-winx64\bin

#import tkinter as tk
from tkinter import ttk
import os
import sys

dos_command = ''
global var_status_cumulative

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
    # global var_host
    # global var_port
    # global var_username
    # global var_password
    # global var_database
    # global var_schema
    #
    # global dos_command

    vinfo = ""
    vinfo = vinfo + '\n' + "SOURCE ENVIRONMENT VARIABLES"
    vinfo = vinfo + '\n' + " dbname=" + var_database.get()
    vinfo = vinfo + '\n' + " user=" + var_username.get()
    vinfo = vinfo + '\n' + " password=*********"
    vinfo = vinfo + '\n' + " port=" + str(var_port.get())
    vinfo = vinfo + '\n' + " host=" + var_host.get()
    vinfo = vinfo + '\n' + " schema=" + var_schema.get()

    vinfo = vinfo + "\n\n TARGET ENVIRONMENT VARIABLES"
    vinfo = vinfo + '\n' + "dbname=" + var_target_database.get()
    vinfo = vinfo + '\n' + " user=" + var_target_username.get()
    vinfo = vinfo + '\n' + " password=*********"
    vinfo = vinfo + '\n' + " port=" + str(var_target_port.get())
    vinfo = vinfo + '\n' + " host=" + var_target_host.get()
    vinfo = vinfo + '\n' + " schema=" + var_target_schema.get()

    vinfo = vinfo + "\n\n Export Options:(bcp options)"
    vinfo = vinfo + '\n' + "Path mysql:" + var_mysql_path.get()
    vinfo = vinfo + '\n' + "Putput folder:" +var_output_folder.get()

    vinfo = vinfo + "\n\n Import options (Copy command"
    vinfo = vinfo + '\n' + "s3 bucket:" + var_target_input_s3_bucket.get()
    vinfo = vinfo + '\n' + "IAM Role:" + var_iam_role.get()
    vinfo = vinfo + '\n' + "Copy Command Options:" + var_copy_cmd_options.get()

    vinfo = vinfo + "\n\n Other:"
    vinfo = vinfo + '\n' + "Status online:"+ var_status_oneline.get()
    vinfo = vinfo + '\n' + "Status cumulative:" + var_status_cumulative.get()


    print(vinfo)
    messagebox.showinfo('List of inputs from user',vinfo)


def test_connectivity(p_popup_msg=1):

    mdict=pack_mssql_var()
    cm=connect_to_mssql_dict(mdict)
    
    if cm!=-1:
        if p_popup_msg==1:
            messagebox.showinfo('Connectivity Test Source', 'Successfull')
            print('Connection to MS SQL', ' OK', time.ctime())
            return (1)
    else:
        if p_popup_msg==1:
            messagebox.showerror('Connectivity Test Source', 'Failure')
        print('Connection to MS SQL', ' ERROR', time.ctime())
        return (-1)


def test_connectivity_target(p_popup_msg=1):

    #oldcr=connect_to_red_param( var_target_host.get(),str(var_target_port.get()),var_target_username.get(),var_target_password.get(),var_target_database.get())
    rdict = build_redshift_dict()
    cr=connect_to_red_dict(rdict)

    if cr!=-1:
        if p_popup_msg == 1:
            messagebox.showinfo('Connectivity test TARGET', 'Successfull')
        print('test_connectivity_target:Connection to redshift', 'Successfull', time.ctime())
        return (1)
    else:
        if p_popup_msg == 1:
            messagebox.showerror('Connectivity Test Target', 'Failure')
        print('test_connectivity_target:Connection to redshift:', '***ERRROR***', time.ctime())
        return (-1)


def test_connectivity_all(p_popup_msg=1):
    
    a=test_connectivity(p_popup_msg)
    b=test_connectivity_target(p_popup_msg)

    if a==1 and b==1:
        return(1) #successfull
    else: 
        return(-1) #failure
    

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

#1=empty(pending) 2=populated 3=all

def call_util_list_tables(p_what_do_you_want):

    cr=connect_to_red_param(var_target_host.get(), str(var_target_port.get()), var_target_username.get(),var_target_password.get(), var_target_database.get())

    if p_what_do_you_want==1: #"empty":
        list_tables = red_list_empty_tables(cr, var_target_schema.get())
    elif p_what_do_you_want ==2: # "populated":
        list_tables = red_list_pop_tables(cr, var_target_schema.get())
    elif p_what_do_you_want ==3: # "all":
        list_tables = red_list_all_tables(cr, var_target_schema.get())
    else:
        list_tables=[]

    if list_tables==[]:
        messagebox.showinfo('List of tables','Empty list, either there is an error or you do not have tables that meet the conditions listed')
    else:
        ltext=''
        for counter,x_table in enumerate(list_tables):
                ltext=ltext + str(counter) + ':' + str(x_table) +'\n'

        file_path = f"{var_output_folder.get()}\list_tables2.txt"
        File_object = open(file_path,'w')
        File_object.write(ltext)
        ltextprev=ltext[0:800]
        messagebox.showinfo('List of Tables', f'Preview shown. Full results saved in this file:{file_path} \n\n {ltextprev} ')


def call_util_list_tables_a():
    call_util_list_tables(1)

def call_util_list_tables_b():
    call_util_list_tables(2)

def call_util_list_tables_c():
    call_util_list_tables(3)


def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))



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
    var_target_input_s3_bucket.set ('s3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift')


    var_status_oneline.set("Results coming up...")
    var_status_cumulative.set("File not generated yet -Please click CallMysqldump.exe to start")
    var_mysql_path.set("C:\\temp\\mysqlsetup\\mysql-8.0.17-winx64\\bin")
    var_output_folder.set("C:\\temp")
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
    var_target_password.set("!INtbW3PtXGgMS")  # passwords must be provided by user
    var_target_database.set("bwpgmsdev")  # this can be a dropbox
    var_target_schema.set('***Pick One***')
    var_target_input_s3_bucket.set ('s3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift')

    var_mysql_path.set("C:\\Program Files\\Microsoft SQL Server\\Client SDK\\ODBC\\130\\Tools\\Binn")

def set_other_defaults():
    var_status_oneline.set("Results coming up...")
    var_status_cumulative.set("File not generated yet -Please click CallMysqldump.exe to start")
    var_output_folder.set("C:\\temp")

    var_s3_bucket.set('s3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift')
    var_iam_role.set("arn:aws:iam::599920608012:role/role-redshift-bwp-gms-data-dev")
    var_copy_cmd_options.set("gzip  json \'auto\'")
    RadiobuttonInt.set(1)
    var_run_mode.set('execute')
    var_target_schema.set('qptm_gc')


def pack_copy_cmd_entries():

    copy_config = {
        "s3_bucket": var_s3_bucket.get()
        ,"iam_role": var_iam_role.get()
        ,"options": var_copy_cmd_options.get()
    }
    return(copy_config)


def create_source_data_frame(window):

    fr_configure_db = Frame(window, bg="grey96",highlightthickness=2,highlightcolor="cyan",highlightbackground="RoyalBlue4")

    # All the labels
    Label(fr_configure_db, text="SOURCE DATABASE (MICROSOFT SQL").grid(row=0, column=0)
    Label(fr_configure_db, text="Host").grid(row=1, column=0, sticky=E)
    Label(fr_configure_db, text="Port").grid(row=2, column=0, sticky=E)
    Label(fr_configure_db, text="Username").grid(row=3, column=0, sticky=E)
    Label(fr_configure_db, text="Password").grid(row=4, column=0, sticky=E)
    Label(fr_configure_db, text="Database").grid(row=5, column=0, sticky=E)
    #Label(fr_configure_db, text="Schema").grid(row=6, column=0, sticky=E)

    # Label(fr_configure_db, text="bcp.exe path").grid(row=7, column=0, sticky=E)
    # Label(fr_configure_db, text="JSON Output Folder:").grid(row=8, column=0, sticky=E)

    # All the inputs
    input_port = Entry(fr_configure_db, textvariable=var_port)
    # 2 is a ttk.Combobox
    comboHost = ttk.Combobox(fr_configure_db, values=["x,y"], textvariable=var_host,  width=50)
    input_username = Entry(fr_configure_db, textvariable=var_username)
    input_password = Entry(fr_configure_db, textvariable=var_password, show="*")
    # 5 is a ttk.Combobox
    comboDatabase = ttk.Combobox(fr_configure_db,values=['QPTM_GC','QPTM_GS','Gasquest'], textvariable=var_database, width=30)
    comboSchema = ttk.Combobox(fr_configure_db,values=['dbo','pending_read_Schemas',], textvariable=var_schema, width=30)


    # input_path_exe = Entry(fr_configure_db, textvariable=var_mysql_path, width=50)
    # input_out_folder = Entry(fr_configure_db, textvariable=var_output_folder, width=50)

    comboHost.grid(row=1, column=2, sticky=W)
    input_port.grid(row=2, column=2, sticky=W)
    input_username.grid(row=3, column=2, sticky=W)
    input_password.grid(row=4, column=2, sticky=W)
    comboDatabase.grid(row=5, column=2, sticky=W)  # Database is line$3
    # comboSchema.grid(row=6, column=2, sticky=W)  # Database is line$3
    # input_path_exe.grid(row=7, column=2, sticky=W)
    # input_out_folder.grid(row=8, column=2, sticky=W)

    return (fr_configure_db)

def create_import_options_frame(window):

    fr_configure_db = Frame(window, bg="grey96",highlightthickness=2,highlightcolor="cyan",highlightbackground="RoyalBlue4")

    # All the labels
    Label(fr_configure_db, text="Options for bcp export command").grid(row=0, column=0,columnspan=2)
    Label(fr_configure_db, text="   ").grid(row=1, column=0, sticky=E)
    Label(fr_configure_db, text="bcp.exe path").grid(row=2, column=0, sticky=E)
    Label(fr_configure_db, text="Output Folder for JSON:").grid(row=3, column=0, sticky=E)
    Label(fr_configure_db, text="   ").grid(row=4, column=0, sticky=E)
    Label(fr_configure_db, text="Source Schema").grid(row=5, column=0, sticky=E)
    Label(fr_configure_db, text="*Schemas are not part of the connection string, thus are included in this section.").grid(row=6, column=0, sticky=W,columnspan=4)

    input_path_exe = Entry(fr_configure_db, textvariable=var_mysql_path, width=70)
    input_out_folder = Entry(fr_configure_db, textvariable=var_output_folder, width=70)
    comboSchema = ttk.Combobox(fr_configure_db,values=['dbo','pending_read_Schemas',], textvariable=var_schema, width=30)

    input_path_exe.grid(row=2, column=1, sticky=W)
    input_out_folder.grid(row=3, column=1, sticky=W)
    comboSchema.grid(row=5, column=1, sticky=W)  # Database is line$3

    return (fr_configure_db)

def create_target_data_frame(window):

    fr_configure_db = Frame(window, bg="grey96",highlightthickness=2,highlightcolor="cyan",highlightbackground="RoyalBlue4")

    # All the labels
    Label(fr_configure_db, text="TARGET DATABASE (REDSHIFT)").grid(row=0, column=0)

    Label(fr_configure_db, text="Host").grid(row=1, column=0, sticky=E)
    Label(fr_configure_db, text="Port").grid(row=2, column=0, sticky=E)
    Label(fr_configure_db, text="Username").grid(row=3, column=0, sticky=E)
    Label(fr_configure_db, text="Password").grid(row=4, column=0, sticky=E)
    Label(fr_configure_db, text="Database").grid(row=5, column=0, sticky=E)

    # Label(fr_configure_db, text="*Schema will be automatically appended to the end").grid(row=6, column=2, sticky=E)
    # Label(fr_configure_db, text=" ").grid(row=7, column=0, sticky=E)
    # Label(fr_configure_db, text="S3 path").grid(row=8, column=0, sticky=E)

    # All the inputs
    input_port = Entry(fr_configure_db, textvariable=var_target_port)
    comboHost = ttk.Combobox(fr_configure_db, values=["127.0.0.1","bwp-gms-data-dev.cwkyrxg2o0p1.us-east-1.redshift.amazonaws.com"], textvariable=var_target_host,width=75)
    input_username = Entry(fr_configure_db, textvariable=var_target_username)
    input_password = Entry(fr_configure_db, textvariable=var_target_password, show="*")
    input_database = Entry(fr_configure_db, textvariable=var_target_database)

    comboHost.grid(row=1, column=1, sticky=W)
    input_port.grid(row=2, column=1, sticky=W)
    input_username.grid(row=3, column=1, sticky=W)
    input_password.grid(row=4, column=1, sticky=W)
    input_database.grid(row=5, column=1, sticky=W)
    #row7 is an empty line

    return (fr_configure_db)



def create_redshift_options_frame(window):

    fr_configure_copycmd = Frame(window, highlightthickness=2,highlightcolor="cyan",highlightbackground="RoyalBlue4")

    # All the labels
    Label(fr_configure_copycmd, text="Options for redshift import command").grid(row=0, column=0,columnspan=2)
    Label(fr_configure_copycmd, text=" ").grid(row=1, column=0, columnspan=2)

    Label(fr_configure_copycmd, text="S3 Bucket").grid(row=2, column=0, sticky=E)
    Label(fr_configure_copycmd, text="IAM Role").grid(row=3, column=0, sticky=E)
    Label(fr_configure_copycmd, text="Options").grid(row=4, column=0, sticky=E)
    Label(fr_configure_copycmd, text="   ").grid(row=5, column=0, sticky=E)
    Label(fr_configure_copycmd, text="Schema").grid(row=6, column=0, sticky=E)
    Label(fr_configure_copycmd, text="*Schemas are not part of the connection string, thus are included in this section.").grid(row=7, column=0, sticky=W,columnspan=4)

    # All the inputs
    input_s3_bucket = Entry(fr_configure_copycmd, textvariable=var_s3_bucket,width=70)
    input_iam_role = Entry(fr_configure_copycmd, textvariable=var_iam_role, width=70)
    input_options = Entry(fr_configure_copycmd, textvariable=var_copy_cmd_options, width=30)
    comboSchemas = ttk.Combobox(fr_configure_copycmd, values=["gasquest","gasquest2019","gmsdl","gmsdw","qptm_gc","qptm_gs"], textvariable=var_target_schema, width=60)

    input_s3_bucket.grid(row=2,column=1,sticky=W)
    input_iam_role.grid(row=3,column=1,sticky=W)
    input_options.grid(row=4,column=1,sticky=W)
    #empty_line=======row=5======================
    comboSchemas.grid(row=6, column=1, sticky=W)

    return (fr_configure_copycmd)

def create_general_filter_frame(window):

#select wich tables to filter (this is a general filter)
    global var_filter_word

    fr_general_filter = Frame(window, bg="grey96",highlightthickness=2,highlightcolor="cyan",highlightbackground="RoyalBlue4")

    title=Label(fr_general_filter, text="Which tables you want to process?",  bg="grey96")
    emptyline = Label(fr_general_filter, text="              ", bg="grey96")

    filter_option1=Radiobutton(fr_general_filter, text="All tables", variable=RadiobuttonInt, value=1)
    filter_option2=Radiobutton(fr_general_filter, text="Empty in target (pending)", variable=RadiobuttonInt, value=2)
    filter_option3=Radiobutton(fr_general_filter, text="Filter by word (Case Sensitive)==>", variable=RadiobuttonInt, value=3)


#This adds the ability to specify a word
    var_filter_word=StringVar()
    input_filter_word = Entry(fr_general_filter, textvariable=var_filter_word, width=20)


    title.grid(row=1, column=0,columnspan=2,sticky=N+S+E+W)
    filter_option1.grid(row=2, column=0, sticky=W)
    filter_option2.grid(row=3, column=0, sticky=W)
    filter_option3.grid(row=4, column=0, sticky=W)
    # label_filter_word.grid(row=3, column=1, sticky=W)
    emptyline.grid(row=5,column=0)
    input_filter_word.grid(row=4,column=1,sticky=W)

    # labelRunMode.grid(row=6, column=0, sticky=E)
    # ComboRunMode.grid(row=6, column=1, sticky=W)

    return fr_general_filter


#BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
# FRAME FOR BUTTON
#BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB

def create_fr_button_connectivity():

    fr_button = Frame(window, bg="grey96",highlightthickness=2,highlightcolor="cyan",highlightbackground="RoyalBlue4")

    Button(fr_button, text='Set local defaults', command=set_local_putty_default).grid(row=1, column=1, sticky=W)
    Button(fr_button, text='Set server defaults', command=set_server_default).grid(row=1, column=2, sticky=W, pady=4)
#    Label(fr_button, text="empty line  ", foreground="grey96", bg="grey96").grid(row=1, column=3, sticky=W)
    Button(fr_button, text='Test Connectivity source', command=test_connectivity).grid(row=1, column=3,sticky=W)
    Button(fr_button, text='Test Connectivity target', command=test_connectivity_target).grid(row=1, column=4,sticky=W)
#    Button(fr_button, text='Show values for variables now', command=show_entry_fields).grid(row=1, column=4)

    return(fr_button)


def create_fr_button_steps():

    fr_button_steps = Frame(window, bg="grey96",highlightthickness=2,highlightcolor="cyan",highlightbackground="RoyalBlue4")

    title     = Label(fr_button_steps, text="Migration process Steps", bg="grey96").grid(row=1, column=1, sticky=W+N+S+E)
    emptyline = Label(fr_button_steps, text="              ", bg="grey96").grid(row=2, column=1, sticky=W)
    labelRunMode = Label(fr_button_steps, text="Run Mode:").grid(row=3, column=0, sticky=E)
    ComboRunMode = ttk.Combobox(fr_button_steps, values=["print", "execute"], textvariable=var_run_mode, width=20).grid(row=3, column=1, sticky=W)
    emptyline2 = Label(fr_button_steps, text="              ", bg="grey96").grid(row=4, column=1, sticky=W)

    btn01=Button(fr_button_steps, text='Step1:  Export using MSSQL bcp', command=step1_export)
    btn02=Button(fr_button_steps, text='Step2:  Compress using gzip', command=step2_compress)
    btn03=Button(fr_button_steps, text='Step3:  Upload to s3', command=step3_upload)
    btn04=Button(fr_button_steps, text='Step4:  Run Redshift copy command', command=step4_run_redshift)
    btn05=Button(fr_button_steps, text='Step5:  RowCount Validation', command=step5_validate)

    btn01.grid(row=5, column=1, sticky=W)
    btn02.grid(row=6, column=1, sticky=W)
    btn03.grid(row=7, column=1, sticky=W)
    btn04.grid(row=8, column=1, sticky=W)
    btn05.grid(row=9, column=1, sticky=W)


    return (fr_button_steps)


def create_fr_utility_lists():

    fr_button_utility_lists = Frame(window, highlightthickness=2,highlightcolor="cyan",highlightbackground="RoyalBlue4")

    Label(fr_button_utility_lists, text="UTILITIES",  bg="grey96").grid(row=0, column=0,columnspan=99)
    Button(fr_button_utility_lists, text='List all empty tables (in target)', command=call_util_list_tables_a).grid(row=1, column=1, sticky=W, pady=4)
    Button(fr_button_utility_lists, text='List all populated tables (in target)', command=call_util_list_tables_b).grid(row=2, column=1, sticky=W, pady=4)
    Button(fr_button_utility_lists, text='List all tables (in target)', command=call_util_list_tables_c).grid(row=7, column=1, sticky=W, pady=4)


    return (fr_button_utility_lists)


#=======================================================================================
# FRAME FOR RESULTS
#=======================================================================================

# def create_fr_results():
#
#     fr_result=Frame(window,bg="gray96") #bg="RoyalBlue4"
#
#     #text_result=Text(fr_result) #,background="gray96") #, textvariable=var_status_cumulative) #, text='Status goes here', width=800, relief='sunken',textvariable=var_status_cumulative) #7
#     text_result=Text(fr_result)
#     text_result.insert(INSERT,"line1")
#     #text_result.pack()
#     wg_out_message=Message(fr_result, text='Initial test', width=1200, relief='sunken',textvariable=var_status_cumulative) #7
#     textscrollbar = Scrollbar(fr_result)
#     textscrollbar.config(command=text_result.yview)
#     text_result.config(yscrollcommand=textscrollbar.set)
#     text_result.insert(INSERT,"MYSQL dump flat file preview will be displayed here")

#
#     Label(fr_result, text="     ").grid(row=2, column=0, sticky=E)
#     wg_out_message.grid(row=0, column=0, columnspan=8,sticky=W)  # The columnspan is important because it makes it very wide.
#
#     return(fr_result)

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
        "mssql_server":var_host.get()
        ,"mssql_user":var_username.get()
        ,"mssql_database":var_database.get()
        ,"mssql_port": str(var_port.get())
        ,"mssql_password":var_password.get()
        ,"mssql_schema":var_schema.get()
    }

    return(mssql_conn_dict)

def step1_export():
    mdict=pack_mssql_var()
    v_mode=var_run_mode.get()

    conn_mssql=connect_to_mssql_dict(mdict)
    #print('debug:dictionary for bcp is',mdict)
    #print('debug run mode is:', var_run_mode.get())

    conn_redshift = connect_to_red_param(var_target_host.get(), str(var_target_port.get()), var_target_username.get(),
                                         var_target_password.get(), var_target_database.get())

    lt= mssql_list_tables_to_export(p_conn_mssql=conn_mssql, v_source_schema=var_schema.get(), p_conn_redshift=conn_redshift,
                                                          p_target_schema=var_target_schema.get(), p_filter=get_filter_value())

    print('step1_export:list tables to export:',lt)
    rc=mssql_bcp_export_schema_json(p_conn_mssql=conn_mssql, p_mssql_conn_dict=mdict, p_list_tables=lt, p_mode= v_mode,p_exe_path=var_mysql_path.get(),p_output_folder=var_output_folder.get())
    if rc!=1:
        messagebox.showerror ('Error',rc)


def step2_compress():
    path_json_files=var_output_folder.get()+ "\\"+ var_database.get()
    #dos_compress_files(path_json_files)

    try:
        rc=dos_compress_files(path_json_files)
        messagebox.showinfo ('Gzip compression',f'Gzip files compressed successfully {path_json_files}')
    except:
        messagebox.showerror('Gzip compression','Failure')
        #print('step2_compress():pending good error management')

def step3_upload():
    messagebox.showinfo('This is a manuaal step by now, copy all the files to the s3 bucket')

def step4_run_redshift():
    rdict = build_redshift_dict()
    ccc=pack_copy_cmd_entries() #copy command configuration

    #print('debug run mode is:',var_run_mode.get())
    #pending work read preferences from interface and sent it along (mode/filter)
    red_run_copy_command_schema(cdict=rdict, copy_cmd_cfg=ccc,p_run_mode=var_run_mode.get(), p_filter=get_filter_value())


def get_filter_value():

    if RadiobuttonInt.get() == 1:
        v_filter_value="all"
    elif RadiobuttonInt.get() == 2:
        v_filter_value="pending"
    elif RadiobuttonInt.get() == 3:
        v_filter_value = var_filter_word.get()
        if len(v_filter_value)==0:
           messagebox.showerror('Read filter value', f'Please specify a word for the filter')
           v_filter_value=-1

    print('The value for the filter is:', v_filter_value)
    return (v_filter_value)


def step5_validate():

    mdict=pack_mssql_var()
    conn_mssql=connect_to_mssql_dict(mdict)
    rdict = build_redshift_dict()
    conn_redshift=connect_to_red_dict(rdict)

    #pending validation of connection
    v_filter_value = get_filter_value()
    if v_filter_value != -1:
        valid_main_schema_rowcount_only(conn_src=conn_mssql, dic_src=mdict, conn_tgt=conn_redshift,
                                        p_target_schema=var_target_schema.get(), p_filter=v_filter_value)
    else:
        print('step5_validate/get_filter_value():problem getting filter value')


    # ctest=test_connectivity_all(p_popup_msg=0)
    #
    # if ctest==1:
    #     #template for call is def valid_main_schema_rowcount_only(conn_src, dic_src, conn_tgt, p_target_schema, p_filter):
    #     v_filter_value=get_filter_value()
    #     if v_filter_value!=-1:
    #         valid_main_schema_rowcount_only(conn_src=conn_mssql, dic_src=mdict, conn_tgt=conn_redshift, p_target_schema=var_target_schema.get(), p_filter=v_filter_value)
    #     else:
    #         print('step5_validate/get_filter_value():problem getting filter value')
    # else:
    #     messagebox.showinfo('Title','There is an error connecting to the databases, please check parameters and try again\n Not running comparison')
    #     show_entry_fields()


#                  _
#                 (_)
#  _ __ ___   __ _ _ _ __
# | '_ ` _ \ / _` | | '_ \
# | | | | | | (_| | | | | |
# |_| |_| |_|\__,_|_|_| |_|
#
#



list_schema_combo = []
list_schema_combo.append("Configure and then load Schemas")
# def clear_message():
#     global var_status_cumulative
#     var_status_cumulative.set('')


window = Tk()
window.configure(background='gray96')
window.geometry("1300x800+20+20")
vmycellsize = "                           "
Label(window, text=vmycellsize).grid(row=0, column=0)
vmycellsize = "                           "
Label(window, text=vmycellsize).grid(row=0, column=1)
vmycellsize = "                           "
Label(window, text=vmycellsize).grid(row=0, column=2)

#Title and Header with Instructions
window.winfo_toplevel().title("PYMYMIGRATION-Export data from Microsoft to the Redshift")

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
var_target_input_s3_bucket = StringVar() #'s3://bwp-gms-mikedey-special-chars/AsIsMigration/Redshift')

#OTHER =========================================================================================
var_status_oneline = StringVar()
var_status_cumulative = StringVar()
var_mysql_path = StringVar()
var_output_folder = StringVar()
var_flags = StringVar()
var_run_mode = StringVar()


#redshift comand
var_s3_bucket = StringVar()
var_iam_role = StringVar()
var_copy_cmd_options = StringVar()

RadiobuttonInt = IntVar()

#grid_configuration

fr_left_source_db = create_source_data_frame(window)
fr_right_source_db = create_target_data_frame(window)

fr_redshift=create_redshift_options_frame(window)
fr_import_options=create_import_options_frame(window)

fr_general_filter=create_general_filter_frame(window)


fr_button_connectivity=create_fr_button_connectivity()
fr_button_steps=create_fr_button_steps()
#fr_results=create_fr_results()
fr_utility_lists=create_fr_utility_lists()

set_local_putty_default()
set_other_defaults()

fr_left_source_db.grid(row=1, column=1,columnspan=2,sticky=W+E+N+S)
fr_right_source_db.grid(row=1, column=3,columnspan=2,sticky=W+E+N+S)

fr_button_connectivity.grid(row=2,column=1,columnspan=4,sticky=W+E+N+S)

fr_import_options.grid(row=3,column=1,columnspan=2,sticky=W+E+N+S)
fr_redshift.grid(row=3,column=3,columnspan=2,sticky=W+E+N+S)

fr_general_filter.grid(row=4,column=1,sticky=N+S) #+E+N+S)
fr_button_steps.grid(row=4,column=2,sticky=W) # columnspan=2,rowspan=1)

fr_utility_lists.grid(row=4,column=3,columnspan=2,rowspan=2,sticky=W+E+N+S)

emptyline = Label(window, text="              ", bg="grey96")
emptyline.grid(row=5, column=0)
emptyline.grid(row=6, column=0)


b_quit=Button(window, text='Quit', command=window.quit)
b_quit.config( height = 3, width = 2 )
b_quit.grid(row=7, column=4, sticky=W+E+N+S)

#Placement

#text_result.grid(row=3,column=0 ,columnspan=8,sticky=W) #The columnspan is important because it makes it very wide.
sepline_result1=Label(window, text=' ')
sepline_result2=Label(window, text=' ')
sepline_result1.grid(row=8,column=0)
sepline_result2.grid(row=9,column=0)
#fr_result.grid(row=8,column=0) #frame#3


window.mainloop()


# except:
#     PrintException()