import psycopg2
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import slack
import os
import csv
import datetime



if __name__ == "__main__":
    print("Doing work...")


client = slack.WebClient(token='xoxb-227200048215-768930300244-QeZyS1dCA2fWoQLv844lKpcg')
fromDate = datetime.date.today() - datetime.timedelta(days=7)
toDate = datetime.date.today() - datetime.timedelta(days=5)
print ("from date: ", fromDate)
print ("to date: " , toDate)
smtpServer='smtp1.ezypay.local'      
fromAddr='no-reply@ezypay.com'         
toAddr=['wyeliong.lam@ezypay.com']     
server = smtplib.SMTP(smtpServer,25)
invoiceid=''
updateInvoiceScripts=''



def callSlack(resultSet,updateInvoiceScripts,updateInvoiceTransactionScripts):
    
    string="*Invoices in Processing for " + str(datetime.date.today())+ "*\n\n" + resultSet + "\n\n" + "*Update Invoice Scripts*\n\n" + updateInvoiceScripts + "\n\n" + "*Update InvoiceTransaction Scripts*\n\n" + updateInvoiceTransactionScripts
    response = client.chat_postMessage(
        channel='#testchannel',text=string)



def queryPayment(invoiceid):

    queryPaymentInvoiceScripts=''
    queryPaymentInvoiceTransactionScripts=''
    invoicetransactionid=''

    try:
        connection = psycopg2.connect(user = "wye_liong",
                                      password = "97jm47mib7s7K2u9J32A",
                                      host = "sydney-cb-cluster.cluster-ro-crp0dlky68om.ap-southeast-2.rds.amazonaws.com",
                                      port = "5432",
                                      database = "ezpayment")
        cursor = connection.cursor()
        postgreSQL_select_Query1 = "select id,status,invoiceid, invoicetransactionid, concat('UPDATE invoice set status =''',case WHEN status ='SUCCESS' THEN 'PAID' WHEN status ='FAILURE' THEN 'FAILED' ELSE status END,''' where id = ''', invoiceid,''';' ) as UpdateInvoice,"
        postgreSQL_select_Query1 += "concat('UPDATE invoicetransaction set status =''', case WHEN status ='FAILURE' THEN 'FAILED' ELSE status END ,''' where id = ''',invoicetransactionid,''';' ) as UpdateTransaction "
        postgreSQL_select_Query1 += "from payment where invoiceid in (%s)" % (invoiceid)


        print("Query", postgreSQL_select_Query1)

        cursor.execute(postgreSQL_select_Query1)
        resultSet = cursor.fetchall()


        for row in resultSet:
            if row == resultSet[-1]:
                queryPaymentInvoiceScripts += row[4]
                queryPaymentInvoiceTransactionScripts += row[5]
                invoicetransactionid += ("'" + row[3] + "'")
                
            else:
                queryPaymentInvoiceScripts += row[4] + ","
                queryPaymentInvoiceTransactionScripts += row[5] + ","
                invoicetransactionid += ("'" + row[3] + "', ")


    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data from PostgreSQL", error)
    finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    return queryPaymentInvoiceScripts,queryPaymentInvoiceTransactionScripts,invoicetransactionid



def queryLedger(invoicetransactionid):

    try:
        connection = psycopg2.connect(user = "wye_liong",
                                      password = "97jm47mib7s7K2u9J32A",
                                      host = "sydney-cb-cluster.cluster-ro-crp0dlky68om.ap-southeast-2.rds.amazonaws.com",
                                      port = "5432",
                                      database = "ezaccounting")
        cursor = connection.cursor()
        postgreSQL_select_Query1 = "select distinct(invoicetransactionid),invoiceid from ezledgerentries "
        postgreSQL_select_Query1 += "where invoicetransactionid in (%s)" % (invoicetransactionid)
        postgreSQL_select_Query1 += "and invoicetransactionid not in (select invoicetransactionid from ezledgerentries where transactiontype = 'failed_payment' and invoicetransactionid in (%s))" % (invoicetransactionid)


        print("Query", postgreSQL_select_Query1)

        cursor.execute(postgreSQL_select_Query1)
        resultSet = cursor.fetchall()

        print (resultSet)

    except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data from PostgreSQL", error)
    finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    return resultSet


try:
        connection = psycopg2.connect(user = "wye_liong",
                                      password = "97jm47mib7s7K2u9J32A",
                                      host = "sydney-cb-cluster.cluster-ro-crp0dlky68om.ap-southeast-2.rds.amazonaws.com",
                                      port = "5432",
                                      database = "ezbilling")
        cursor = connection.cursor()
        postgreSQL_select_Query1 = "select m.tradingname, c.firstname, c.lastname, i.id as invoiceid, cast (i.invoicedate as varchar), i.status,i.currencycode,i.amount, it.id as invoicetransactionid, it.status as invoicetransactionstatus, cast (it.createdon as varchar) InvoiceTransCreatedOn, "
        postgreSQL_select_Query1 += "concat('UPDATE invoice set status =''',case WHEN it.status ='SUCCESS' THEN 'PAID' ELSE it.status END,''' where id = ''',i.id,''';' ) from invoice i join invoicetransaction it on it.invoiceid = i.id join customer c on cast (c.id as varchar) = i.issueto join merchant m on cast (m.id as varchar) = i.issueby "
        postgreSQL_select_Query1 += "where i.status = 'PROCESSING' and i.invoicedate < '%s' and i.duedate < '%s' and i.billingrelationshiptype ='MERCHANT_TO_CUSTOMER'" % (toDate, toDate)
        postgreSQL_select_Query1 += "and i.invoicedate > '%s' and i.id not in " % (fromDate)
        postgreSQL_select_Query1 += "(select i.id from invoice i inner join invoicetransaction it on  cast(i.id as varchar) = cast(it.invoiceid as varchar) "
        postgreSQL_select_Query1 += "where i.status = 'PROCESSING' and invoicedate < '%s' and i.invoicedate > '%s' " % (toDate, fromDate)
        postgreSQL_select_Query1 += "and it.createdon::date >= '%s' and i.duedate < '%s' and billingrelationshiptype ='MERCHANT_TO_CUSTOMER')" % (toDate, toDate)
        postgreSQL_select_Query1 += "order by i.invoicedate, it.createdon, i.id"

        print("Query", postgreSQL_select_Query1)

        cursor.execute(postgreSQL_select_Query1)
        resultSet = cursor.fetchall()

except (Exception, psycopg2.Error) as error :
        print ("Error while fetching data from PostgreSQL", error)
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


        for row in resultSet:
            if row == resultSet[-1]:
                updateInvoiceScripts += row[11]
                invoiceid += ("'" + row[3] + "'")
            else:
                updateInvoiceScripts += row[11] + ","
                invoiceid += ("'" + row[3] + "', ")

        queryPaymentInvoiceScripts,queryPaymentInvoiceTransactionScripts,invoicetransactionid=queryPayment(invoiceid)
        clawbackResult = queryLedger(invoicetransactionid)
        callSlack(str(resultSet),str(queryPaymentInvoiceScripts), str(queryPaymentInvoiceTransactionScripts), str(clawbackResult))