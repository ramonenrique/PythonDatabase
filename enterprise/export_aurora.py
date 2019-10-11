#all of these need inputs

#Custom-made libraries
from aurora_functions import *
from db_generic import *
from tkinter import *
from tkinter import messagebox
from pymysql import connect

#better to set this in the environment (computer) fixed permanentlyset path=% path %;C:\temp\mysqlsetup\mysql-8.0.17-winx64\bin

#import tkinter as tk
from tkinter import ttk
import os
import sys


def show_entry_fields():
    global var_database
    global var_host
    global var_port
    global var_username
    global var_password
    global dos_command

    print("Host and port :", var_host.get(), var_port.get())
    print("Login credentials:",var_username.get(),"secret password")
    print("database selected:",var_database.get())



def test_connectivity():
    global var_host
    global var_port
    global var_username
    global var_password

    #use hardcoded value for default database bwpgmsdefault
    try:
        conn_aurora = connect(host=var_host.get(), port=var_port.get(),user=var_username.get(),passwd=var_password.get(), db='bwpgmsdefault')
        messagebox.showinfo("Connectivity Test", "You are connected successfully-Will load all schemas now")
        load_schema_list()
    except:
        messagebox.showerror("Connectivity Test", "Connection FAILED")



def load_schema_list():
    global var_host
    global var_port
    global var_username
    global var_password

    conn_aurora = connect(host=var_host.get(), port=var_port.get(), user=var_username.get(),passwd=var_password.get(), db='bwpgmsdefault')
    list_schema_combo = ["<***All Schemas***>"]
    list_schema_db=db_list_all_schemas(conn_aurora)
    list_schema_combo=list_schema_combo+list_schema_db

    comboDomains.config(values = list_schema_combo)
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

def call_dos_mysqldump_onedb(p_database):
    global var_status_cumulative
    global var_status_cumulative
    global var_host
    global var_port
    global var_username
    global var_password
    global wg_out_message
    global var_mysql_path
    global var_output_folder
    global msg_out_feedback

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

try:  # covers the whole program

    dos_command = ''
    global var_status_cumulative

    list_schema_combo=[]
    list_schema_combo.append("Configure and then load Schemas")
    # def clear_message():
    #     global var_status_cumulative
    #     var_status_cumulative.set('')

    window=Tk()
    window.configure(background='gray96')
    window.geometry("1200x800+50+50")
    vmycellsize="                           "
    Label(window, text=vmycellsize).grid(row=0,column=0)
    vmycellsize="                           "
    Label(window, text=vmycellsize).grid(row=0,column=1)
    vmycellsize="                           "
    Label(window, text=vmycellsize).grid(row=0,column=2)


    var_database=StringVar()  # Holds a string; default value ''
    var_host=StringVar()  # Holds a string; default value ''
    var_port=IntVar()  # Holds an int; default value 0
    var_username=StringVar()  # Holds a string; default value ''
    var_password=StringVar()  # Holds a string; default value ''
    var_status_oneline=StringVar()
    var_status_cumulative=StringVar()
    var_mysql_path=StringVar()
    var_output_folder=StringVar()
    var_flags=StringVar()

    def set_direct_default():
        var_host.set("bwp-gms-dev-aurora-0.cztswafip9oo.us-east-1.rds.amazonaws.com")
        var_port.set(3306)
        var_username.set("admin")
        var_password.set("MoosoBFWZ8BScw") #passwords must be provided by user
        var_database.set("<<Pending Schema load>>")  # this can be a dropbox
        var_status_oneline.set("Results coming up...")
        var_status_cumulative.set("File not generated yet-Please click CallMysqldump.exe to start")
        var_mysql_path.set("C:\\Program Files\\MySQL\\MySQL Workbench 8.0 CE")
        var_output_folder.set("C:\\temp\\")
        var_flags.set(' --force --skip-add-drop-table --lock-all-tables=FALSE  --skip-column-statistics --no-data --skip-quote-names  --no-data=true --dump-date --routines=false --no-create-db=TRUE')

    def set_local_putty_default():
        var_host.set("127.0.0.1")
        var_port.set(3308)
        var_username.set("admin")
        var_password.set("MoosoBFWZ8BScw") #passwords must be provided by user
        var_database.set("<<Pending Schema load>>")  # this can be a dropbox
        var_status_oneline.set("Results coming up...")
        var_status_cumulative.set("File not generated yet -Please click CallMysqldump.exe to start")
        var_mysql_path.set("C:\\temp\\mysqlsetup\\mysql-8.0.17-winx64\\bin")
        var_output_folder.set("C:\\temp\\")
        var_flags.set(' --force --skip-add-drop-table --lock-all-tables=FALSE  --skip-column-statistics --no-data --skip-quote-names  --no-data=true --dump-date --routines=false --no-create-db=TRUE')


    #Title and Header with Instructions
    window.winfo_toplevel().title("PYMYSQLDUMP-Export schema for Aurora MYSQL database")
    # vStaticInstructions=""
    # wg_static1_msg=Message(window, text=vStaticInstructions, width=600, relief='sunken')
    # wg_static1_msg.grid(row=0,column=0 ,columnspan=7,sticky=W) #The columnspan is important because it makes it very wide.

    #IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII
    #input_host=Entry(window,textvariable=var_host)
    #This frames will have only two columns, labels to the left and input values to the right
    #IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII

    fr_input_user=Frame(window,bg="gray96")

    #All the labels
    Label(fr_input_user, text="Host").grid(row=1,column=0,sticky=E)
    Label(fr_input_user, text="Port").grid(row=2,column=0,sticky=E)
    Label(fr_input_user, text="Username").grid(row=3, column=0,sticky=E)
    Label(fr_input_user, text="Password").grid(row=4,column=0,sticky=E)
    Label(fr_input_user, text="Database").grid(row=5,column=0,sticky=E)
    Label(fr_input_user, text="Mysql.exe path").grid(row=6,column=0,sticky=E)
    Label(fr_input_user, text="Output Folder:").grid(row=7,column=0,sticky=E)
    Label(fr_input_user, text="]").grid(row=8,column=2,sticky=E)
    Label(fr_input_user, text="Flags").grid(row=8,column=0,sticky=E)

    #All the inputs
    input_port=Entry(fr_input_user,textvariable=var_port).grid(row=1,column=2,sticky=W)
    #2 is a combobox
    comboHost=ttk.Combobox(fr_input_user,values=["bwp-gms-dev-aurora-0.cztswafip9oo.us-east-1.rds.amazonaws.com","bwp-gms-dev-aurora-dwtestbed-temp.cztswafip9oo.us-east-1.rds.amazonaws.com","127.0.0.1"],textvariable=var_host,width=60)
    input_username=Entry(fr_input_user,textvariable=var_username).grid(row=3,column=2,sticky=W)
    input_password=Entry(fr_input_user,textvariable=var_password,show="*").grid(row=4,column=2,sticky=W)
    #5 is a combobox
    comboDomains=ttk.Combobox(fr_input_user,values=list_schema_combo,textvariable=var_database,width=60)
    #["accounting","billing",  "contract", "pipelinemodel","entity","location","nomination","rates","rfs"],textvariable=var_database,width=60)
    input_path_exe=Entry(fr_input_user,textvariable=var_mysql_path,width=100).grid(row=6,column=2,sticky=W)
    input_out_folder=Entry(fr_input_user,textvariable=var_output_folder,width=100).grid(row=7,column=2,sticky=W)
    input_flags=Entry(fr_input_user,textvariable=var_flags,width=150).grid(row=8,column=2,sticky=W,columnspan=2,rowspan=2)

    comboHost.grid(row=2, column=2,sticky=W)
    comboDomains.grid(row=5,column=2,sticky=W) #Database is line$3

    fr_input_user.grid(row=1,column=0) #frame#1

    #BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
    # FRAME FOR BUTTON
    #BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB

    fr_button=Frame(window,bg="gray96")

    Button(fr_button, text='Set local defaults', command=set_local_putty_default).grid(row=1,column=1,sticky=W, pady=4)
    Button(fr_button, text='Set server defaults', command=set_direct_default).grid(row=2,column=1,sticky=W, pady=4)
    Button(fr_button, text='Test connectivity', command=test_connectivity).grid(row=3,column=1,sticky=W, pady=4)

    Button(fr_button, text='Load Schema list', command=load_schema_list).grid(row=4,column=1,sticky=W, pady=4)
    Label(fr_button, text="     ").grid(row=5,column=1,sticky=E)
    Label(fr_button, text="     ").grid(row=6,column=1,sticky=E)

    Button(fr_button, text='Call MysqlDump.exe', command=call_dos_mysqldump).grid(row=7,column=1,sticky=W, pady=4)
    Button(fr_button, text='Quit',command=window.quit).grid(row=8, column=1, sticky=W, pady=4)
    # Button(fr_button, text='Show Var Selected values',command=show_entry_fields).grid(row=9, column=1, sticky=W, pady=4)

    # needed only for testing Button(fr_button, text='Window Update',command=window.update).grid(row=9, column=1, sticky=W, pady=4)


    fr_button.grid(row=1,column=2) #frame#2 (trowards the right)



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

    #Placement
    wg_out_message.grid(row=0,column=0 ,columnspan=8,sticky=W) #The columnspan is important because it makes it very wide.
    Label(fr_result, text="     ").grid(row=2,column=0,sticky=E)
    #text_result.grid(row=3,column=0 ,columnspan=8,sticky=W) #The columnspan is important because it makes it very wide.

    sepline_result1=Label(window, text=' ')
    sepline_result2=Label(window, text=' ')
    sepline_result1.grid(row=3,column=0)
    sepline_result2.grid(row=4,column=0)

    fr_result.grid(row=6,column=0) #frame#3


    set_direct_default()
    window.mainloop()


except: # catch *all* exceptions
    v_err = sys.exc_info()[0]
    print("<p>Error: %s</p>" % v_err)
