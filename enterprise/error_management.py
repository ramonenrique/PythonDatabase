import os, sys, traceback
from tkinter import messagebox

def exception_handler():
    exc_type, exc_obj, exc_traceback = sys.exc_info() #this helps split the components right away
    tb = sys.exc_info()[-1]
    stk = traceback.extract_tb(tb, 1)
    v_error_function = stk[0][2]

    v_msg='\n [Exception Type]:' + str(exc_type)
    v_msg = v_msg + '\n\n [Error message]:' + str(exc_obj.args[0])
    v_msg = v_msg + '\n\n [File Name:]' +  os.path.split(exc_traceback.tb_frame.f_code.co_filename)[1]
    v_msg = v_msg + '\n\n [Function]:' + v_error_function
    v_msg = v_msg + '\n\n [Line Number]:' + str(exc_traceback.tb_lineno)
    print(v_msg)
    raise

def exception_handler_screen():
    print('SCREEN/INTERFACE ERROR HANDLING')
    exc_type, exc_obj, exc_traceback = sys.exc_info() #this helps split the components right away
    tb = sys.exc_info()[-1]
    stk = traceback.extract_tb(tb, 1)
    v_error_function = stk[0][2]

    v_msg='\n [Exception Type]:' + str(exc_type)
    v_msg = v_msg + '\n\n [Error message]:' + str(exc_obj.args[0])
    v_msg = v_msg + '\n\n [File Name:]' +  os.path.split(exc_traceback.tb_frame.f_code.co_filename)[1]
    v_msg = v_msg + '\n\n [Function]:' + v_error_function
    v_msg = v_msg + '\n\n [Line Number]:' + str(exc_traceback.tb_lineno)
    print(v_msg)
    messagebox.showerror('Error',v_msg)


