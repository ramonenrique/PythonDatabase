#all of these need inputs



#better to set this in the environment (computer) fixed permanentlyset path=% path %;C:\temp\mysqlsetup\mysql-8.0.17-winx64\bin

from tkinter import *
from tkinter import ttk
import os
import sys

dos_command=''

def step1_export():
    print("hello")

def step2_compress():
    print("hello")

def step3_upload():
    print("hello")

def step4_run_redshift():
    print("hello")

def show_entry_fields():
    global var_out_feedback
    global var_sqldump_file
    global var_database
    global var_host
    global var_port
    global var_username
    global var_password
    global dos_command

    dos_command=f'mysqldump --host={var_host.get()} --port={var_port.get()} --user={var_username.get()} --password={var_password.get()} --force --databases {var_database.get()} --lock-all-tables=FALSE --add-drop-table --result-file={var_output_folder.get()}baseline_{var_database.get()}.sql --log-error=mysql_error_{var_database.get()}.txt --skip-column-statistics --no-data --skip-quote-names --skip-add-drop-table  --no-data=true --dump-date --routines=false --no-create-db=TRUE'


    print("Host and port :", var_host.get(), var_port.get())
    print("Login credentials:",var_username.get(),"secret password")
    print("database selected:",var_database.get())
    print("dos command:",dos_command)

    # e1.delete(0, END)
    # e2.delete(0, END)

def call_step1():
    print('step1')

def call_step2():
    print('step2')

def call_step3():
    print('step3')


def call_dos_mysqldump():
    global var_out_feedback
    global var_sqldump_file
    global var_database
    global var_host
    global var_port
    global var_username
    global var_password
    global wg_out_message
    global var_mysql_path
    global var_output_folder

    #it should be automatically setnt to var_database comboDomains.get()

    v_full_path_result_file=f'{var_output_folder.get()}baseline_{var_database.get()}.sql'

    dos_command=f'mysqldump.exe --host={var_host.get()} --port={var_port.get()} --user={var_username.get()} --password={var_password.get()} --force --databases {var_database.get()} --lock-all-tables=FALSE --result-file={v_full_path_result_file} --log-error=mysql_error_{var_database.get()}.txt --skip-column-statistics --no-data --skip-quote-names --skip-add-drop-table --skip-quote-names --no-data=true --dump-date --routines=false --no-create-db=TRUE'
    print(dos_command)

    var_out_feedback.set("Running mysqldump")
    var_sqldump_file.set("")
    wg_out_message.configure(background='yellow')
    wg_sqldump_result.delete(1.0, END)

    try:
        os.chdir(f'{var_mysql_path.get()}')
        print('Folder for mysql is valid')

        returnvalue=os.system(dos_command)
        print('reach this point(a)')
        vfile = open(f'{var_output_folder.get()}baseline_{var_database.get()}.sql', 'r')
        v_file_contents = vfile.read()
        var_sqldump_file.set(v_file_contents)
        wg_sqldump_result.insert(END,v_file_contents)
        xlen = len(v_file_contents)
        if xlen >= 1280:
            var_out_feedback.set(f'OK:Valid database and file generated successfully\nResults saved to {v_full_path_result_file}')
            wg_out_message.configure(background='green2')
        else:
            var_out_feedback.set("ERROR:File not generated successfully, please check settings and try again")
            wg_out_message.configure(background='red2')

    except OSError as err:
        print("OS error: {0}".format(err))
        var_out_feedback.set("OS error: {0}".format(err))
        print("OS error: {0}".format(err))
        print('reach this point(B)')
    except:
        print("Unexpected error running Mysqldump.exe:", sys.exc_info()[0])
        var_out_feedback.set("Unexpected error running Mysqldump.exe:")
        raise


# def clear_message():
#     global var_out_feedback
#     var_out_feedback.set('')

window=Tk()
window.geometry("1300x800+20+20")
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
var_out_feedback=StringVar()
var_sqldump_file=StringVar()
var_mysql_path=StringVar()
var_output_folder=StringVar()

def set_direct_default():
    var_host.set("bwp-gms-dev-aurora-0.cztswafip9oo.us-east-1.rds.amazonaws.com")
    var_port.set(3306)
    var_username.set("admin")
    var_password.set("MoosoBFWZ8BScw") #passwords must be provided by user
    var_database.set("accounting")  # this can be a dropbox
    var_out_feedback.set("Results coming up...")
    var_sqldump_file.set("File not generated yet")
    var_mysql_path.set("C:\\Program Files\\MySQL\\MySQL Workbench 8.0 CE")
    var_output_folder.set("C:\\temp\\")

def set_local_putty_default():
    var_host.set("127.0.0.1")
    var_port.set(3308)
    var_username.set("admin")
    var_password.set("MoosoBFWZ8BScw") #passwords must be provided by user
    var_database.set("accounting")  # this can be a dropbox
    var_out_feedback.set("Results coming up...")
    var_sqldump_file.set("File not generated yet")
    var_mysql_path.set("C:\\temp\\mysqlsetup\\mysql-8.0.17-winx64\\bin")
    var_output_folder.set("C:\\temp\\")



#IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII
#input_host=Entry(window,textvariable=var_host)
#This frames will have only two columns, labels to the left and input values to the right
#IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII

def create_data_frame(window):

    fr_left_source_db=Frame(window,bg="grey96")

    #All the labels
    Label(fr_left_source_db, text="SOURCE DATABASE").grid(row=0,column=0)
    Label(fr_left_source_db, text="Host").grid(row=1,column=0,sticky=E)
    Label(fr_left_source_db, text="Port").grid(row=2,column=0,sticky=E)
    Label(fr_left_source_db, text="Username").grid(row=3, column=0,sticky=E)
    Label(fr_left_source_db, text="Password").grid(row=4,column=0,sticky=E)
    Label(fr_left_source_db, text="Database").grid(row=5,column=0,sticky=E)

    Label(fr_left_source_db, text="bcp.exe path").grid(row=6,column=0,sticky=E)
    Label(fr_left_source_db, text="JSON Output Folder:").grid(row=7,column=0,sticky=E)

    #All the inputs
    input_port=Entry(fr_left_source_db,textvariable=var_port).grid(row=1,column=2,sticky=W)
    #2 is a ttk.Combobox
    comboHost=ttk.Combobox(fr_left_source_db,values=["legacy-db.bwpmlp.org","127.0.0.1"],textvariable=var_host,width=60)
    input_username=Entry(fr_left_source_db,textvariable=var_username).grid(row=3,column=2,sticky=W)
    input_password=Entry(fr_left_source_db,textvariable=var_password,show="*").grid(row=4,column=2,sticky=W)
    #5 is a ttk.Combobox
    comboDomains=ttk.Combobox(fr_left_source_db,values=["accounting","billing",  "contract", "pipelinemodel","entity","location","nomination","rates","rfs"],textvariable=var_database,width=60)
    input_path_exe=Entry(fr_left_source_db,textvariable=var_mysql_path,width=70).grid(row=6,column=2,sticky=W)
    input_out_folder=Entry(fr_left_source_db,textvariable=var_output_folder,width=70).grid(row=7,column=2,sticky=W)

    comboHost.grid(row=2, column=2,sticky=W)
    comboDomains.grid(row=5,column=2,sticky=W) #Database is line$3

    return(fr_left_source_db)


fr_left_source_db=create_data_frame(window)
fr_right_source_db=create_data_frame(window)


fr_left_source_db.grid(row=1, column=0)
fr_right_source_db.grid(row=1, column=1)



#BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
# FRAME FOR BUTTON
#BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB

#BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
# FRAME FOR BUTTON
#BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB

fr_button=Frame(window,bg="grey90")

Button(fr_button, text='Set local defaults', command=set_local_putty_default).grid(row=1,column=1,sticky=W, pady=4)
Button(fr_button, text='Set server defaults', command=set_direct_default).grid(row=2,column=1,sticky=W, pady=4)
Label(fr_button, text="empty line  ",foreground="grey96",bg="grey96").grid(row=3, column=0, sticky=W)
Label(fr_button, text="empty line  ",foreground="grey96",bg="grey96").grid(row=4, column=0, sticky=W)

Button(fr_button, text='Step1:  Export using MSSQL bcp'     ,command=step1_export).grid(row=5,column=1,sticky=W, pady=4)
Button(fr_button, text='Step2:  Compress using gzip'        ,command=step2_compress).grid(row=6, column=1, sticky=W, pady=4)
Button(fr_button, text='Step3:  Upload to s3'               ,command=step3_upload).grid(row=7, column=1, sticky=W, pady=4)
Button(fr_button, text='Step4:  Run Redshift copy command'  ,command=step4_run_redshift).grid(row=8, column=1, sticky=W, pady=4)

fr_button.grid(row=2,column=0) #frame#2 (trowards the right)


#=======================================================================================
# FRAME FOR RESULTS
#=======================================================================================
fr_result=Frame(window,bg="grey96")


wg_sqldump_result=Text(fr_result,background="grey96") #, textvariable=var_sqldump_file) #, text='Status goes here', width=800, relief='sunken',textvariable=var_sqldump_file) #7
wg_out_message=Message(fr_result, text='Initial test', width=800, relief='sunken',textvariable=var_out_feedback) #7
textscrollbar = Scrollbar(fr_result)
textscrollbar.config(command=wg_sqldump_result.yview)
wg_sqldump_result.config(yscrollcommand=textscrollbar.set)
wg_sqldump_result.insert(END,"MYSQL dump flat file preview will be displayed here")

#Placement
wg_out_message.grid(row=0,column=0 ,columnspan=4,sticky=W) #The columnspan is important because it makes it very wide.
Label(fr_result, text="     ").grid(row=2,column=0,sticky=E)
wg_sqldump_result.grid(row=3,column=0 ,columnspan=8,sticky=W) #The columnspan is important because it makes it very wide.

fr_result.grid(row=3,column=0)


set_direct_default()
window.mainloop()

