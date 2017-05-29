# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 16:28:04 2017

@author: skuchibhotla
"""
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 07 05:53:24 2017

@author: skuchibhotla
"""
from Tkinter import *
from ScrolledText import ScrolledText
import pandas as pd
def readBlocks(fileName,blk_size):
    fd = open(fileName,'r')
    head = fd.readline()
    head = head[:-1]
    dictn = {}
    for heading in head.split():
        dictn[heading] = []
    blks = []
    headings = head.split('\t')
    i = 0
    for record in fd.readlines():
        record = record[:-1]
        cols = record.split('\t')
        j = 0
        for col in cols:
            dictn[headings[j]].append(col)
            j += 1
        i += 1
        if  i%blk_size is 0:
            blks.append(dict(dictn))
            for heading in head.split():
                dictn[heading] = []
    if i % blk_size != 0:
        blks.append(dict(dictn))
    fd.close()
    return blks

root = Tk()
root.configure(background='black')
root.title("Merge Join Algorithm with Given Queries")
root.resizable(width=False, height=False)
frame = Frame(root)
queryvar = StringVar()
ql = StringVar()
frame.grid(row=2,column=0,columnspan=4)
text = ScrolledText(frame,wrap="word")
text.grid(row=2)
text.config(font=("Courier", 12))
text.configure(background='white',foreground='black')
d = "C:/Users/skuch/Desktop/Courses/8340_DB2/Project/Tables/"
branchNYC = readBlocks(d+"NewYork_Branch.txt",7)
text.insert(INSERT,"\n")
#text.insert(INSERT, len(branchNYC))

accountNYC = readBlocks(d+'NewYork_Account.txt',10)
text.insert(INSERT,"\n")
#text.insert(INSERT, len(accountNYC))

depositorNYC = readBlocks(d+'NewYork_Depositor.txt',15)
text.insert(INSERT,"\n")
#text.insert(INSERT, len(depositorNYC))

customerHOU = readBlocks(d+'Houston_Customer.txt',8)
text.insert(INSERT,"\n")
#text.insert(INSERT, len(customerHOU))

branchSFO = readBlocks(d+"SanFrancisco_Branch.txt",7)
text.insert(INSERT,"\n")
#text.insert(INSERT, len(branchSFO))

accountSFO = readBlocks(d+'SanFrancisco_Account.txt',10)
text.insert(INSERT,"\n")
#text.insert(INSERT, len(accountSFO))

accountOMA = readBlocks(d+'Omaha_Account.txt',10)
text.insert(INSERT,"\n")
#text.insert(INSERT, len(accountOMA))

depositorOMA = readBlocks(d+'Omaha_Depositor.txt',15)
text.insert(INSERT,"\n")
#text.insert(INSERT, len(depositorOMA))

def query1():
    text.configure(state="normal")
    text.delete('1.0',END)
    ql.set('QUERY-->')
    queryvar.set('Find name, street, city information for all account holders.')
    result = {}
    text.insert(INSERT,"\n\tCustomer Table from HOU(8 blocks)\n\tDepositor Table from NYC(4 blocks)\n")
    text.insert(INSERT,"\nMerge Join implemented 8 times\n")
    text.insert(INSERT,"\nJoining 2 blocks of customer table with 2 blocks of depositor table.\n")
    result = mergeJoin(customerHOU[0:2],depositorNYC[0:2],'Customer_Name',1,'*',result)
    text.insert(INSERT,"\nJoining 2 blocks of customer table with 2 blocks of depositor table.\n")
    result = mergeJoin(customerHOU[0:2],depositorNYC[2:4],'Customer_Name',1,'*',result)
    text.insert(INSERT,"\nJoining 2 blocks of customer table with 2 blocks of depositor table.\n")
    result = mergeJoin(customerHOU[2:4],depositorNYC[0:2],'Customer_Name',1,'*',result)
    text.insert(INSERT,"\nJoining 2 blocks of customer table with 2 blocks of depositor table.\n")
    result = mergeJoin(customerHOU[2:4],depositorNYC[2:4],'Customer_Name',1,'*',result)
    text.insert(INSERT,"\nJoining 2 blocks of customer table with 2 blocks of depositor table.\n")
    result = mergeJoin(customerHOU[4:6],depositorNYC[0:2],'Customer_Name',1,'*',result)
    text.insert(INSERT,"\nJoining 2 blocks of customer table with 2 blocks of depositor table.\n")
    result = mergeJoin(customerHOU[4:6],depositorNYC[2:4],'Customer_Name',1,'*',result)
    text.insert(INSERT,"\nJoining 2 blocks of customer table with 2 blocks of depositor table.\n")
    result = mergeJoin(customerHOU[6:8],depositorNYC[0:2],'Customer_Name',1,'*',result)
    text.insert(INSERT,"\nJoining 2 blocks of customer table with 2 blocks of depositor table.\n")
    result = mergeJoin(customerHOU[6:8],depositorNYC[2:4],'Customer_Name',1,'*',result)
    if len(result[list(result)[0]]) != 0:
        text.insert(INSERT,"\n")
        text.insert(INSERT, pd.DataFrame(result).to_string(index=False))
    text.configure(state="disabled")
    
def query2():
    
    text.configure(state="normal")
    text.delete('1.0',END)
    ql.set('QUERY-->')
    queryvar.set('Find customer name and balance for any account at Chinatown branch.')
    text.insert(INSERT,"\n\tDepositor Table from NYC(4 blocks)\n\tAccount Table from SFO(1 block)\n")
    text.insert(INSERT,"\nMerge Join implemented 2 times\n")
    
    result = {}
    text.insert(INSERT,"\nJoining 3 blocks of depositor table with 1 blocks of account table.\n")
    result = mergeJoin(depositorNYC[0:3],accountSFO,'Account_Number',0,['Customer_Name','Balance'],result)
    text.insert(INSERT,"\nJoining 1 blocks of depositor table with 1 blocks of account table.\n")
    result = mergeJoin(depositorNYC[3:4],accountSFO,'Account_Number',0,['Customer_Name','Balance'],result)
    if len(result[list(result)[0]]) != 0:
        df = pd.DataFrame(result)
        text.insert(INSERT,"\n")
        text.insert(INSERT, df[['Customer_Name','Balance']].to_string(index=False))
    text.configure(state="disabled")
def query3():
    text.configure(state="normal")
    text.delete('1.0',END)
    ql.set('QUERY-->')
    queryvar.set('Find customer street and city information account number ‘458696321’. ')
    text.insert(INSERT,"\n\tDepositor Table from NYC(4 blocks)\n\tTable with Account Number 'A10352'(1 block)\n")
    
    text.insert(INSERT,"\nMerge Join implemented 2 times\n")
    account458696321 = [{'Account_Number':['458696321']}]        
    result = {}
    text.insert(INSERT,"\nJoining 3 blocks of depositor table with 1 blocks of account table.\n")
    result = mergeJoin(depositorNYC[0:3],account458696321,'Account_Number',1,'*',result)
    text.insert(INSERT,"\nJoining 1 blocks of depositor table with 1 blocks of account table.\n")
    result = mergeJoin(depositorNYC[3:4],account458696321,'Account_Number',1,'*',result)
    if len(result[list(result)[0]]) != 0:
        text.insert(INSERT,"\n")
        text.insert(INSERT, pd.DataFrame(result).to_string(index=False))
    accountNew = [result]
    text.insert(INSERT,"\n\n\tCustomer Table from HOU(8 blocks)\n\tAccount Table formed above(1 block)\n")
    text.insert(INSERT,"\nHash Join called 3 times\n")
    
    result = {}
    text.insert(INSERT,"\nJoining 3 blocks of customer table with 1 blocks of account table.\n")
    result = mergeJoin(customerHOU[0:3],accountNew,'Customer_Name',1,['Customer_Street','Customer_City'],result)
    text.insert(INSERT,"\nJoining 3 blocks of customer table with 1 blocks of account table.\n")
    result = mergeJoin(customerHOU[3:6],accountNew,'Customer_Name',1,['Customer_Street','Customer_City'],result)
    text.insert(INSERT,"\nJoining 2 blocks of customer table with 1 blocks of account table.\n")
    result = mergeJoin(customerHOU[6:8],accountNew,'Customer_Name',1,['Customer_Street','Customer_City'],result)
    if len(result[list(result)[0]]) != 0:
        df = pd.DataFrame(result)
        text.insert(INSERT,"\n")
        text.insert(INSERT,df[['Customer_Street','Customer_City']].to_string(index=False))
        
        text.insert(INSERT,"\n")
    text.configure(state="disabled")
def query4():
    
    text.configure(state="normal")
    text.delete('1.0',END)
    ql.set('QUERY-->')
    queryvar.set('Select customer name,street and city for depositors in omaha.')
    text.insert(INSERT,"Joining:\n\tCustomer Table from HOU(8 blocks)\n\tDepositor Table from OMA(2 blocks)\n")
    text.insert(INSERT,"\nMerge Join called 4 times\n")
    print len(depositorOMA)
    result = {}
    result = mergeJoin(customerHOU[0:2],depositorOMA[0:2],'Customer_Name',1,'*',result)
    result = mergeJoin(customerHOU[2:4],depositorOMA[0:2],'Customer_Name',1,'*',result)
    result = mergeJoin(customerHOU[4:6],depositorOMA[0:2],'Customer_Name',1,'*',result)
    result = mergeJoin(customerHOU[6:8],depositorOMA[0:2],'Customer_Name',1,'*',result)
    if len(result[list(result)[0]]) != 0:
        df = pd.DataFrame(result)
        text.insert(INSERT,"\n")
        text.insert(INSERT, df.to_string(index=False))
    text.configure(state="disabled")


l = Label(textvariable=queryvar)
l.grid(row=6,column=0,columnspan=5,pady=20)
l.config(font=("Courier", 14),background='black',foreground='green')
q = StringVar()   
q.set('5') 
b = Button(root,background='black',border='2',fg='white',text='Query 1',command=query1)
b.grid(row=1,column=0,pady=20)
b = Button(root,background='black',border='2',fg='white',text='Query 2',command=query2)
b.grid(row=1,column=1,pady=20)

b = Button(root,background='black',border='2',fg='white',text='Query 3',command=query3)
b.grid(row=1,column=2,pady=20)

b = Button(root,background='black',border='2',fg='white',text='Query 4',command=query4)
b.grid(row=1,column=3,pady=20)


#SEMI-JOIN ALGORITHM
def semi_join(table1,table2,attribute):
    df1 = pd.DataFrame(table1)
    df2 = pd.DataFrame(table2)
    return df1[df1[attribute].isin(df2[attribute])].to_dict('list')
    
#READ-BLOCKS

#BLOCK-NESTED LOOP AlGORITHM

def mergeJoin(r,s,att,semijoin,select_att,result):
    total_res = {}
    a = {}
    b = {}
    rdf = pd.DataFrame(r[0:1][0])
    rhead = list(rdf)
    for heading in rhead:
        a[heading] = []
    for Br in r:
        for heading in rhead:
            for i in Br[heading]:
                a[heading].append(i)
    sdf = pd.DataFrame(s[0:1][0])
    shead = list(sdf)
    for heading in shead:
        b[heading] = []
    for Bs in s:
        for heading in shead:
            for i in Bs[heading]:
                b[heading].append(i)
    r = a
    s = b
    rdf = pd.DataFrame(r)
    sdf = pd.DataFrame(s)
    """
    text.insert(INSERT,"\n")
    text.insert(INSERT, rdf.to_string(index=False))
    text.insert(INSERT,"\n\n")
    text.insert(INSERT, sdf.to_string(index=False))
    text.insert(INSERT,"\n")
    """
    #print r,s
    
    rdf = rdf.sort_values([att],ascending=[1])
    sdf = sdf.sort_values([att],ascending=[1])
    
    text.insert(INSERT,"\nTables are sorted.\n")
    """
    text.insert(INSERT, rdf.to_string(index=False))
    text.insert(INSERT,"\n\n")
    text.insert(INSERT, sdf.to_string(index=False))
    text.insert(INSERT,"\n")
    """
    r = rdf.to_dict('list')
    s = sdf.to_dict('list')
    pr = 0
    ps = 0
    prn = len(r[att])
    psn = len(s[att])
    rhead = list(rdf)
    while pr<prn and ps<psn:
        ts = {}
        for heading in shead:
            ts[heading] = [s[heading][ps]]
        Ss = {}
        for heading in shead:
            Ss[heading] = [s[heading][ps]]
        
        ps += 1
        done = False
        while not done and ps<psn:
            tsd = {}
            for heading in shead:
                tsd[heading] = [s[heading][ps]]
            if tsd[att] == ts[att]:
                for heading in shead:
                    Ss[heading].append(tsd[heading][0])
                ps += 1
            else:
                done = True
        tr = {}
        for heading in rhead:
            tr[heading] = [r[heading][pr]]
        while pr<prn and tr[att][0] < ts[att][0] :
            pr += 1
            
            tr = {}
            if pr<prn:
                for heading in rhead:
                    tr[heading] = [r[heading][pr]]
            else:
                break
        while pr<prn and tr[att][0] == ts[att][0] :
            Brdf = pd.DataFrame(tr)
            Bsdf = pd.DataFrame(Ss)
            Brdictn = {}
            Brhead = list(Brdf)
            Bsdictn = {}
            Bshead = list(Bsdf)
            for ttr in Brdf.iterrows():
                i = 0
                for col in ttr[1]:
                    Brdictn[Brhead[i]] = [col]
                    i += 1
                for tts in Bsdf.iterrows():
                    i = 0
                    for col in tts[1]:
                        Bsdictn[Bshead[i]] = [col]
                        i += 1
                    sj = dict()
                    
                    if semijoin == 0:
                        if Brdictn[att] == Bsdictn[att]:
                            for col,value in Brdictn.iteritems():
                                sj[col] = value
                            for col,value in Bsdictn.iteritems():
                                sj[col] = value
                        
                    else:
                        sj = semi_join(dict(Brdictn),dict(Bsdictn),att)
                        
                    if sj != {} and len(sj[(list(sj))[0]]) != 0:
                        
                        head = list(sj)
                        if result == {}:
                            for heading in head:
                                result[heading] = []
                        if total_res == {}:
                            for heading in head:
                                total_res[heading] = []
                        for heading in head:
                            result[heading].append(sj[heading][0])
                            total_res[heading].append(sj[heading][0])
                        if len(result[head[0]]) == 5:
                            
                            if select_att == '*':
                                text.insert(INSERT,"\n")
                                text.insert(INSERT, pd.DataFrame(result).to_string(index=False))
                            else:
                                df = pd.DataFrame(result)
                                text.insert(INSERT,"\n")
                                text.insert(INSERT, df[select_att].to_string(index=False))
                            text.insert(INSERT, "\n\nBLOCK(OUTPUT) is Full//Clearing//Cleared.\n")
                            
                            for heading in head:
                                result[heading] = []
            pr += 1
            tr = {}
            if pr<prn:
                for heading in rhead:
                    tr[heading] = [r[heading][pr]]
            else:
                break
    return result





#text.configure(state="disabled")

root.mainloop()







