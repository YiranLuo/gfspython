import zclient
import random

client=zclient.ZClient()

#flag=raw_input('Enter "q" to quit- ')
flag=1
files=['file1.txt','file2.txt','file3.txt','file4.txt','file5.txt']
readwhen=[1,4,7,9]
writewhen=[0,2,3,5,8]
deletewhen=[6]

choice=0

while flag!= '0':
  a=client.master.get('filetable').keys()
  a.remove('#garbage_collection#')

  print "a =",a

  #read file
  if a!=[] and choice in readwhen:
   filename=random.choice(a)
   print "read ",filename
   client.read(filename)
   print "---"

  #write file
  if choice in writewhen:
    filename=random.choice(files)
    with open("upload/"+filename,'r') as f:
      data=f.read()
    
    if random.getrandbits(1) and a!=[]:
      print "edit ",writefname,filename
      writefname=random.choice(a) #edit
    else:
      print "write ",writefname,filename
      writefname=str(random.random()).split('.')[1]+".txt" #write

    client.write(writefname, data)
    print "---"

  #delete file
  if a!=[] and choice in deletewhen:
    filename=random.choice(a)
    print "delete ",filename
    client.delete(filename)
    print "---"
  
  choice=random.randint(0,9)

  with open('upload/flag.txt','r') as f:
    flag=f.read().strip('\n')
  
