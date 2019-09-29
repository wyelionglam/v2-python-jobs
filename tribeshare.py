import psycopg2
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime



if __name__ == "__main__":
    print("Doing work...")



fromDate = datetime.date.today() - datetime.timedelta(days=10)
toDate = datetime.date.today() - datetime.timedelta(days=3)
print ("from date: ", fromDate)
print ("to date: " , toDate)
smtpServer='smtp1.ezypay.local'      
fromAddr='no-reply@ezypay.com'         
toAddr=['wyeliong.lam@ezypay.com','YongZheng.Kee@ezypay.com']     
server = smtplib.SMTP(smtpServer,25)
botany='Botany\nTotal PAID invoices for Botany: '
rouse='Rouse Hill \nTotal PAID invoices for Rouse Hill: '
wollongong='Wollongong \nTotal PAID invoices for Wollongong: '
canterbury='Canterbury \nTotal PAID invoices for Canterbury: '




try:
        connection = psycopg2.connect(user = "wye_liong",
                                      password = "97jm47mib7s7K2u9J32A",
                                      host = "sydney-cb-cluster.cluster-ro-crp0dlky68om.ap-southeast-2.rds.amazonaws.com",
                                      port = "5432",
                                      database = "ezbilling")
        cursor = connection.cursor()
        postgreSQL_select_Query1 = "select sum(case when (c.merchantid ='66ceb2d4-773d-4345-85c9-a2cabdad490d') then i.amount else 0 end) as Botany,"
        postgreSQL_select_Query1 += "sum(case when (c.merchantid ='e47e0d1a-2669-42d3-a6df-b9ed49a579f9') then i.amount else 0 end) as RouseHill,"
        postgreSQL_select_Query1 += "sum(case when (c.merchantid ='5e775b39-ecf7-4aa1-8802-ed10a561c318') then i.amount else 0 end) as Canterbury,"
        postgreSQL_select_Query1 += "sum(case when (c.merchantid ='4dc3f31e-3fa8-4df2-aea7-2a990cbafab9') then i.amount else 0 end) as Wollongong "
        postgreSQL_select_Query1 += "from Invoice I, Customer C where I.issueto = CAST(C.id as varchar) and I.invoicedate >= '%s' and I.invoicedate < '%s' and I.referenceinvoiceid is null and i.status = 'PAID' " % (fromDate, toDate)
        postgreSQL_select_Query1 += "and C.merchantid in ('e47e0d1a-2669-42d3-a6df-b9ed49a579f9', '66ceb2d4-773d-4345-85c9-a2cabdad490d', '4dc3f31e-3fa8-4df2-aea7-2a990cbafab9', '5e775b39-ecf7-4aa1-8802-ed10a561c318')" 
        cursor.execute(postgreSQL_select_Query1)
        tribeCount = cursor.fetchall() 
    
        for row in tribeCount:
            botany+=str(row[0])  + '\n\n8.8% = ' + str(int(row[0])*8.8/100) + '\n\nRoyalty - $203.07 - MarketingInternetFee'+ '\n\n\n\n'
            rouse+=str(row[1])  + '\n\n6.6% = ' + str(int(row[1])*6.6/100) + '\n\n\n\n'
            canterbury+=str(row[2])  + '\n\n8.8% = ' + str(int(row[2])*8.8/100) + '\n\nRoyalty - $214.50- MarketingInternetFee'+ '\n\n\n\n'
            wollongong+=str(row[3])  + '\n\n8.8% = ' + str(int(row[3])*8.8/100) + '\n\nRoyalty - $214.50- MarketingInternetFee'
            print("Botany = ", row[0], "\n")
            print("Rouse Hill = ", row[1], "\n")
            print("Canterbury = ", row[2], "\n")
            print("Wollongong = ", row[3], "\n")
except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data from PostgreSQL", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


message='Subject: {}\n\n{}'.format('Tribe Functional Training Gross Share ' + str(fromDate) + ' to ' + str(toDate), botany + rouse + canterbury + wollongong)




server.sendmail(fromAddr, toAddr, message) 
print("Email sent")
server.quit()