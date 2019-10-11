#all of these need inputs

import copy

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

def create_data_frame(window):

    fr_button=tk.Frame(window,bg="grey80")

    tk.Button(fr_button, text='Step1', command=call_step1).grid(row=0,column=0,sticky=tk.W, pady=4)
    tk.Button(fr_button, text='Step2', command=call_step2).grid(row=1,column=0,sticky=tk.W, pady=4)
    tk.Button(fr_button, text='Step3', command=call_step3).grid(row=2,column=0,sticky=tk.W, pady=4)

    tk.Button(fr_button, text='Quit',command=window.quit).grid(row=4, column=0, sticky=tk.W, pady=4)

    tk.Button(fr_button, text='Set local defaults', ).grid(row=5,column=1,sticky=tk.W, pady=4)
    tk.Button(fr_button, text='Set server defaults', ).grid(row=6,column=1,sticky=tk.W, pady=4)

    return(fr_button)

fr_button=create_data_frame(window)
fr_button2=create_data_frame(window)


fr_button.grid(row=0,column=0)
fr_button2.grid(row=0,column=2)


window.mainloop()

