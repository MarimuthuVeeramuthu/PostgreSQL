/*
Source:- copy_table.c
Description:- It will copy a all rows from one table of a database into another table of data base
Author:- Marimuthu V
*/

#include<stdarg.h>
#include<string.h>
#include"libpq-fe.h"

#define QSTRING_LEN 4096
#define ROWCOUNT 1e6
#define SYNC 0
#define ASYNC 1
#define PROCESSING_TYPE SYNC
#define FALSE 0
#define TRUE 1

/* Define ADT for database communication, defined required information for a database connection*/
typedef struct database
{
  PGconn *con;							/* Connection */
  PGresult *res;                                                /* Result set */
  int status;                                                   /* Connection Status (Not used now) */
  int qlen;                                                     /* Query string length in the buffer 'query' */ 
  int qbuflen;                                                  /* Query string buffer length */
  char query[QSTRING_LEN];                                      /* Query buffer */
}database_t;

/* Close the connection of the database and free the occupied resources */
/* It can be used to close of multiple connections */
int free_database(database_t *dbp, ...)
{
  va_list ap;
  database_t *tdbp;

  va_start(ap,dbp);

  while(tdbp = va_arg(ap,database_t*) )
    PQfinish(tdbp->con);

  PQfinish(dbp->con);

  va_end(ap);
}

/* Create a connection to the database */
void init_database(database_t *dbp,const char *cinfo)
{
  dbp->con = PQconnectdb(cinfo);
  dbp->res = NULL;
  dbp->qbuflen = QSTRING_LEN;
  memset(&dbp->query,0,dbp->qbuflen);
  dbp->qlen = 0;
}

/* Delete a table from a database which implements drop table command*/
/* It can be used to delete multiple tables from a database*/
void delete_table(database_t *dbp, ...)
{
  va_list ap;
  char *table;

  va_start(ap,dbp);
  while(table = va_arg(ap,char*) )
  {
    sprintf(dbp->query,"drop table %s%c",table,'\0');
    dbp->res = PQexec(dbp->con,dbp->query);

    if (PQresultStatus(dbp->res) != PGRES_COMMAND_OK)
          fprintf(stderr, "DROP TABLE %s failed: %s", table,PQerrorMessage(dbp->con));
    PQclear(dbp->res);
  }

  va_end(ap);
}

/* Data preparation function to fill the data in the source table */
/* It will be used as callback routine during copy of one table to another,
During the copy, the table row data can be manipulated respect to the business logic */
int data_from_function(char *buffer, int *buflen,void *user_data)
{
  static int i;
  
  if( i == ROWCOUNT )
    return i;
  i++;

  sprintf(buffer,"%d\t%d\t%d%c%c",i,i%3,i%5,'\n','\0');
  *buflen = strlen(buffer);
  
  return 0;
}

/* Data preparation function to fill the data in the source table */
/* It will be used as callback routine during copy of one table to another,During the copy, 
the table row data can be manipulated respect to the business logic */
int data_from_table(char *buffer, int *buflen,void *user_data)
{
/* The received data from source table can be manipulated further for insert into destination table*/ 
return 0;
}

/* Copy table rows from one table to another*/
/* It gets argument destination database, table and source database and table, 
function pointer for further update of received data from source table before written into destination table, 
user_data if anything the caller need to pass it into callback routine data_func*/

/*It reads rows from source table and passed to data_func for further manipulation, and after send the data to destination table*/
/* If the source database table is not provided then it looking for the data from data_fun and written into destination table*/

/* the usage of this function as below*/
/* table_copy(dest_database,dest_table,src_database,src_table,NULL,NULL);
										-Its reads rows one by one from src database table and written into dest database table*/
/* table_copy(dest_database,dest_table,src_database,src_table,data_func,user_data); 
                                        -Its reads rows one by one from src database table and pass the data into data_func for further manipulation and 
										 then written into dest database table*/
/* table_copy(dest_database,dest_table,NULL,NULL,data_func,user_data); 
                                        -If src database table is NULL, so it gets data from data_func and written into dest database table*/

int table_copy(database_t *ddb,char *dtable, database_t *sdb,char *stable, int (*data_func)(char *buffer,int *buflen,void *user_data),void *user_data )
{
  int blen;
  int dcopy;
  char *buffer;

  if (NULL == sdb && NULL == data_func)
    {
      printf("Arguments are Incorrect\n");
      return -1;
    }

  sprintf(ddb->query,"copy %s from stdin%c",dtable,'\0');     
  ddb->res = PQexec(ddb->con,ddb->query);

  if (PQresultStatus(ddb->res) != PGRES_COPY_IN)
    {
      fprintf(stderr, "COPY FROM STDIN command failed: %s", PQerrorMessage(ddb->con));
      PQclear(ddb->res);
      return -2;
    }
  PQclear(ddb->res);
 
  if (sdb)
    {
      sprintf(sdb->query,"copy %s to stdout%c",stable,'\0');     
      sdb->res = PQexec(sdb->con,sdb->query);
  
      if (PQresultStatus(sdb->res) != PGRES_COPY_OUT)
        {
          fprintf(stderr, "COPY TO STDOUT command failed: %s", PQerrorMessage(sdb->con));
          PQclear(sdb->res);
          return -2;
        }
      PQclear(sdb->res);
    }

  do
  {
    dcopy = FALSE;
    ddb->qlen = ddb->qbuflen;
    if (sdb)
      {
        ddb->qlen = PQgetCopyData(sdb->con,&buffer,PROCESSING_TYPE);
        
        if (0 == ddb->qlen)
            break; /* Receive Data in Blocking mode and copying is in-progress*/
        else if (-1 == ddb->qlen)
            break; /* Copying is done */
        else if (-2 == ddb->qlen)
               {
                 /*Error in Copying*/
                 fprintf(stderr, "GETCOPYDATA command failed: %s", PQerrorMessage(sdb->con));
                 return -2;
                }
        else
            {    
                  memcpy(ddb->query,buffer,ddb->qlen);
                  ddb->query[ddb->qlen] = '\0';
                  dcopy = TRUE;
                  PQfreemem(buffer);
            }
      }

    if (data_func && !(data_func(ddb->query,&ddb->qlen,user_data) ) )
      dcopy = TRUE;

    if (dcopy == TRUE)
      PQputCopyData(ddb->con,ddb->query,ddb->qlen);
  
  }while(dcopy == TRUE);
  
  if (-1 == (PQputCopyData(ddb->con,"\\.\n",3) ) )
    {
      fprintf(stderr, "PUTCOPYDATA command failed: %s", PQerrorMessage(ddb->con));
      return -2;
    }
  
  if (-1 == (PQendcopy(ddb->con) ) )
    {
      fprintf(stderr, "ENDCOPY command failed: %s", PQerrorMessage(ddb->con));
      return -2;
    }

  return 0;
}

int main(int argc,char **argv)
{

/*Declare variables for source and destinations databases*/
  database_t sdb,ddb;
  int i;

  if (argc != 3)
    {
      printf("Usage: <command> <dbname=src database name> <dbname=dest database name>\n");
      return -1;
    }

/* Initialize the database connection for source database*/
  init_database(&sdb,argv[1]);
  if (PQstatus(sdb.con) != CONNECTION_OK )
    {
      fprintf(stderr,"Error in Connection: %s\n",PQerrorMessage(sdb.con) );
      free_database(&sdb,NULL);
      return -1;
    }

/* Initialize the database connection for destination database*/
  init_database(&ddb,argv[2]);
  if (PQstatus(ddb.con) != CONNECTION_OK )
    {
      fprintf(stderr,"Error in Connection: %s\n",PQerrorMessage(ddb.con) );
      free_database(&ddb,NULL);
      return -1;
    }

  printf("Databases[%s][%s] Connected\n",argv[1],argv[2]);

/* Delete source, destination table foo,bar respectively to make sure those are doesn't have exiting records for this demonstration*/
  delete_table(&sdb,"foo",NULL);
  delete_table(&ddb,"bar",NULL);

/* Create source, destination table foo,bar respectively to make sure those are exits for this demonstration*/
  PQexec(sdb.con,"create table foo(a int,b int,c int)" );
  PQexec(ddb.con,"create table bar(a int,b int,c int)" );

/* Fill data into foo table*/
  table_copy(&sdb,"foo",NULL,NULL,data_from_function,NULL);
  table_copy(&ddb,"bar",&sdb,"foo",NULL,NULL);

  printf("Table foo copied into bar\n");

/* Close the Connections*/
  free_database(&sdb,&ddb,NULL);

  return 0;
}
