#all of these need inputs



#better to set this in the environment (computer) fixed permanentlyset path=% path %;C:\temp\mysqlsetup\mysql-8.0.17-winx64\bin

import tkinter as tk
from tkinter import ttk
import os
import sys

dos_command=''

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

    # e1.delete(0, tk.END)
    # e2.delete(0, tk.END)

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
    wg_sqldump_result.delete(1.0, tk.END)

    try:
        os.chdir(f'{var_mysql_path.get()}')
        print('Folder for mysql is valid')

        returnvalue=os.system(dos_command)
        print('reach this point(a)')
        vfile = open(f'{var_output_folder.get()}baseline_{var_database.get()}.sql', 'r')
        v_file_contents = vfile.read()
        var_sqldump_file.set(v_file_contents)
        wg_sqldump_result.insert(tk.END,v_file_contents)
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

window=tk.Tk()
window.geometry("1000x800+100+100")
vmycellsize="                           "
tk.Label(window, text=vmycellsize).grid(row=0,column=0)
vmycellsize="                           "
tk.Label(window, text=vmycellsize).grid(row=0,column=1)
vmycellsize="                           "
tk.Label(window, text=vmycellsize).grid(row=0,column=2)


var_database=tk.StringVar()  # Holds a string; default value ''
var_host=tk.StringVar()  # Holds a string; default value ''
var_port=tk.IntVar()  # Holds an int; default value 0
var_username=tk.StringVar()  # Holds a string; default value ''
var_password=tk.StringVar()  # Holds a string; default value ''
var_out_feedback=tk.StringVar()
var_sqldump_file=tk.StringVar()
var_mysql_path=tk.StringVar()
var_output_folder=tk.StringVar()

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


#Title and Header with Instructions
window.winfo_toplevel().title("PYMYSQLDUMP-Export schema for Aurora MYSQL database")
vStaticInstructions="INSTRUCTIONS:\n1-Set the variables to match the configuration of the computer running the software \n2-Pick your database and enter login credentials\n3-Click button to start the program"
wg_static1_msg=tk.Message(window, text=vStaticInstructions, width=600, relief='sunken')
wg_static1_msg.grid(row=0,column=0 ,columnspan=7,sticky=tk.W) #The columnspan is important because it makes it very wide.

#IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII
#input_host=tk.Entry(window,textvariable=var_host)
#This frames will have only two columns, labels to the left and input values to the right
#IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII

fr_input_user=tk.Frame(window,bg="grey80")

#All the labels
tk.Label(fr_input_user, text="Host").grid(row=1,column=0,sticky=tk.E)
tk.Label(fr_input_user, text="Port").grid(row=2,column=0,sticky=tk.E)
tk.Label(fr_input_user, text="Username").grid(row=3, column=0,sticky=tk.E)
tk.Label(fr_input_user, text="Password").grid(row=4,column=0,sticky=tk.E)
tk.Label(fr_input_user, text="Database").grid(row=5,column=0,sticky=tk.E)
tk.Label(fr_input_user, text="Mysql.exe path").grid(row=6,column=0,sticky=tk.E)
tk.Label(fr_input_user, text="Output Folder:").grid(row=7,column=0,sticky=tk.E)

#All the inputs
input_port=tk.Entry(fr_input_user,textvariable=var_port).grid(row=1,column=2,sticky=tk.W)
#2 is a combobox
comboHost=ttk.Combobox(fr_input_user,values=["bwp-gms-dev-aurora-0.cztswafip9oo.us-east-1.rds.amazonaws.com","bwp-gms-dev-aurora-dwtestbed-temp.cztswafip9oo.us-east-1.rds.amazonaws.com","127.0.0.1"],textvariable=var_host,width=60)
input_username=tk.Entry(fr_input_user,textvariable=var_username).grid(row=3,column=2,sticky=tk.W)
input_password=tk.Entry(fr_input_user,textvariable=var_password,show="*").grid(row=4,column=2,sticky=tk.W)
#5 is a combobox
comboDomains=ttk.Combobox(fr_input_user,values=["accounting","billing",  "contract", "pipelinemodel","entity","location","nomination","rates","rfs"],textvariable=var_database,width=60)
input_path_exe=tk.Entry(fr_input_user,textvariable=var_mysql_path,width=100).grid(row=6,column=2,sticky=tk.W)
input_out_folder=tk.Entry(fr_input_user,textvariable=var_output_folder,width=100).grid(row=7,column=2,sticky=tk.W)

comboHost.grid(row=2, column=2,sticky=tk.W)
comboDomains.grid(row=5,column=2,sticky=tk.W) #Database is line$3

fr_input_user.grid(row=1,column=0)

#BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
# FRAME FOR BUTTON
#BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB

fr_button=tk.Frame(window,bg="grey80")

tk.Button(fr_button, text='Call MysqlDump.exe', command=call_dos_mysqldump).grid(row=0,column=1,sticky=tk.W, pady=4)
tk.Button(fr_button, text='Set local defaults', command=set_local_putty_default).grid(row=1,column=1,sticky=tk.W, pady=4)
tk.Button(fr_button, text='Set server defaults', command=set_direct_default).grid(row=2,column=1,sticky=tk.W, pady=4)
tk.Button(fr_button, text='Quit',command=window.quit).grid(row=3, column=1, sticky=tk.W, pady=4)

fr_button.grid(row=1,column=2)

#=======================================================================================
# FRAME FOR RESULTS
#=======================================================================================
fr_result=tk.Frame(window,bg="grey80")


wg_sqldump_result=tk.Text(fr_result,background="grey85") #, textvariable=var_sqldump_file) #, text='Status goes here', width=800, relief='sunken',textvariable=var_sqldump_file) #7
wg_out_message=tk.Message(fr_result, text='Initial test', width=800, relief='sunken',textvariable=var_out_feedback) #7
textscrollbar = tk.Scrollbar(fr_result)
textscrollbar.config(command=wg_sqldump_result.yview)
wg_sqldump_result.config(yscrollcommand=textscrollbar.set)
wg_sqldump_result.insert(tk.END,"MYSQL dump flat file preview will be displayed here")

#Placement
wg_out_message.grid(row=0,column=0 ,columnspan=4,sticky=tk.W) #The columnspan is important because it makes it very wide.
tk.Label(fr_result, text="     ").grid(row=2,column=0,sticky=tk.E)
wg_sqldump_result.grid(row=3,column=0 ,columnspan=8,sticky=tk.W) #The columnspan is important because it makes it very wide.

fr_result.grid(row=3,column=0)


set_direct_default()
window.mainloop()

