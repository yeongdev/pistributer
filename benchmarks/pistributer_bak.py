"""Original backup implementation of `pistributer`.

Created on Tue May 21 15:29:55 2019.

Author: Yeong

Intent:
- a simple Kafka-like local queue
- use plain text files as the storage system

Known limitation:
- if a single active file becomes very large, `read()` loads the whole file into
    memory before splitting it into lines

Basic use case:

        from pistributer import Pistributer
        Pistributer.put('channel.txt', 'hello world')
        pis = Pistributer('channel.txt')
        print(pis.next())

Historical note:
- originally planned for `pluster_core`
- 2019-07-11: added `json()` to only allow JSON file creation

Dependency note:
- importing this file no longer requires `requests`
- the historical `post()` helper falls back to the Python standard library
"""


import os
import json
import random
import sqlite3
import urllib.request

logError = print
logWarning = print
logInfo = print
logDebug = print

class Pistributer:
    def __init__( self, path ):
        if path:
            self.abspath = "" if os.sep not in path else os.path.abspath(".")
            self.path = {}
            self.path['data'] = os.path.abspath( os.path.join( self.abspath, path ) )
            self.path['index'] = os.path.abspath( os.path.join( self.abspath, '{}.{}'.format( path, 'index' ) ) )
            self.path['in_use'] = os.path.abspath( os.path.join( self.abspath, '{}.{}'.format( path, 'in_use' ) ) )
            
            self.__initialQueue()
            self.directDbFlag = False
        # method 1
        # if path:
        #     self.path = {}
        #     self.path['data'] = path
        #     self.path['index'] = '{}.{}'.format( path, 'index' )
        #     self.path['in_use'] = '{}.{}'.format( path, 'in_use' )
            
        #     self.__initialQueue()
        #     self.directDbFlag = False
            
    def json(target_path, string, overwrite=False):
        '''
        make a completely new file with overwriting the current file.
        use this with careful
        '''
        error = None
        
        # Guardrails for safer file writes
        if not target_path.endswith('.json'):
            return Exception( 'Only allow to write to json file > "{}"'.format(target_path) )
        if os.path.isdir(target_path):
            return Exception( 'This is a folder, require a file to write "{}"'.format(target_path) )
        if overwrite != True:
            if os.path.isfile(target_path):
                return Exception( 'File exists "{}"'.format(target_path) )
            
        # Core write path
        try:
            f = open( target_path, "w+" )
            f.write( string )
            f.flush()
            
        except Exception as e:
            error = e
        finally:
            f.close()
            
        if error:
            raise error
        else:
            return True
        
    def new(target_path, string, overwrite=False, sep=''):
        '''
        make a completely new file with overwriting the current file.
        use this with careful
        '''
        error = None
        
        # Guardrails for safer file writes
        if not target_path.endswith('.txt'):
            return Exception( 'Only allow to write to txt file > "{}"'.format(target_path) )
        if os.path.isdir(target_path):
            return Exception( 'This is a folder, require a file to write "{}"'.format(target_path) )
        if overwrite != True:
            if os.path.isfile(target_path):
                return Exception( 'File exists "{}"'.format(target_path) )
            
        # Core write path
        try:
            f = open( target_path, "w+" )
            string += sep
            f.write( string )
            f.flush()
            
        except Exception as e:
            error = e
        finally:
            f.close()
            
        if error:
            raise error
        else:
            return True
        
    def put(target_path, string):
        error = None
        try:
            f = open( target_path, "a+" )
            string += '\n'
            f.write( string )
            f.flush()
            
        except Exception as e:
            error = e
        finally:
            f.close()
            
        if error:
            raise error
        else:
            return True
        
    def post( host_list, json_obj ):
        filter_list = []
        if type( host_list ) != list:
            host_list = [ host_list ]
        
        while True:
            
            checklist = list( set(host_list) - set(filter_list) )
            
            if len( checklist ) < 1:
                logError( 'Try all host, all fail !!! "{}"'.format( str(host_list) ) )
                break
            
            host = random.choice( checklist )
            try:
                data = json.dumps( json_obj ).encode( 'utf-8' )
                request = urllib.request.Request(
                    host,
                    data=data,
                    headers={'Content-Type': 'application/json'},
                )
                return urllib.request.urlopen( request )
            
            except Exception as e:
                logWarning( 'post data facing error "{}"'.format( str(e) ) )
                filter_list.append( host )
                
    def __read_file_by_line(self, file ):
        if os.path.isfile( file ):
            with open( file, 'r' ) as f:
                lineList = f.read( )
            return lineList.split('\n')
    #        try: return set( lineList.split('\n') ) - set( [''] )
    #        except: return set( )
        else:
            return set( )
        
    def __initialQueue( self ):
        
        self.__index = self.getIndex( )
        
        if os.path.isfile( self.path['in_use'] ):
            self.q = list( self.__readInUse() )
            
        else:
            if os.path.isfile( self.path['data'] ):
                os.rename( self.path['data'], self.path['in_use'] )
                self.q = list( self.__readInUse() )
                self.updateIndex( 0 )
                
            else:
                self.q = []
                if os.path.isfile( self.path['index'] ):
                    os.remove( self.path['index'] )
                    
    def __readInUse( self ):
        if os.path.isfile( self.path['in_use'] ):
            return [ i for i in self.__read_file_by_line( self.path['in_use'] ) if i != '' ]
        
        
    def getIndex( self ):
        if os.path.isfile( self.path['index'] ):
            with open( self.path['index'], 'r+' ) as json_file: 
                try:
                    output = json.load( json_file )
                except:
                    return {'index':0}
            return output
        
        else:
            with open( self.path['index'], 'w+' ) as fp:
                json.dump( {'index':0}, fp )
                
            return {'index':0}
        
    def updateIndex( self, N ):
        self.__index['index'] = N
        
        with open( self.path['index'], 'w+' ) as fp:
            json.dump( self.__index, fp )
            
    def increaseIndex( self, N=1 ):
        self.__index['index'] += N
        
        with open( self.path['index'], 'w+' ) as fp:
            json.dump( self.__index, fp )
            
    def decreaseIndex( self, N=1 ):
        self.__index['index'] -= N
        
        if self.__index['index'] < 0:
            self.__index['index'] = 0
        
        with open( self.path['index'], 'w+' ) as fp:
            json.dump( self.__index, fp )
            
    def next( self ):
        
        data = None
        
        if self.isEmpty() == False:
            data = self.q[ self.__index['index'] ]
        else:
            #self.__initialQueue( )
            data = self.q[ self.__index['index'] ]
            
        self.increaseIndex( )
        
        return data
            
    def __checkOutputForDb( self, data ):
        try:
            output = json.loads( data.replace('\\', '\\\\') )
            if os.path.isfile( output['path'] ):
                return True
            else:
                raise Exception( 'the database we are looking for is not exists "{}"'.format( data ) )
                
        except:
            raise Exception( 'fail to convert from json to dictionary "{}"'.format( data ) )
    
    def __readData( self, data ):
        
        try:
            dbpath = data['path']
            tablename = data['tablename']
            rowid = data['ID']
        except Exception as e:
            raise Exception( 'the input is not standardize for read data from trigger.db "{}" > "{}"'.format( str(e), str(data) ) )
        
        if not os.path.isfile( dbpath ):
            logError( 'cannot find dbpath ? this should not happen, might be data polution or data missing happen. "{}"'.format(dbpath) )
            return False
            
        query = 'SELECT * FROM `{}` WHERE `ID`="{}" LIMIT 1;'.format( tablename, rowid )
        error = None
        output = None
        try:
            connection = sqlite3.connect(dbpath)
            cursor = connection.cursor()
            cursor.execute(query)
            output = cursor.fetchone()
        
        except Exception as e:
            error = '"{}" > "{}"'.format(str(e), query)
            
        finally:
            try:
                connection.close()
            except:
                pass
        
        if error:
            raise Exception( error )
        else:
            return output
        
    def nextFromDB( self ):
        if self.directDbFlag == False:
            logInfo( 'direct manipulate data from db is not allow, directDbFlag need to be True but is "{}"'.format(self.directDbFlag) )
            return False
        
        output = self.next()
        
        data = None
        if self.__checkOutputForDb( output ):
            data = self.__readData( json.loads( output.replace('\\', '\\\\') ) )
        
        return { 'pistributer': output, "data": data }
    
    def removeFromDB( self, pdata ):
        if self.directDbFlag == False:
            logInfo( 'direct manipulate data from db is not allow, directDbFlag need to be True but is "{}"'.format(self.directDbFlag) )
            return False
        
        if 'pistributer' not in pdata or 'data' not in pdata:
            logWarning( 'wrong pistributer data input for removeFromDb() "{}"'.format(str(pdata)) )
            return False
        
        if pdata['data'] == None:
            logDebug( 'this data is None, might be the row already not exists, so not need to execute remove "{}"'.format(str(pdata['pistributer'])) )
            return False
        
        pdata['pistributer'] = json.loads( pdata['pistributer'].replace('\\', '\\\\') )
        
        try:
            dbpath = pdata['pistributer']['path']
            tablename = pdata['pistributer']['tablename']
            rowid = pdata['pistributer']['ID']
            
        except Exception as e:
            logError( 'the input is not standardize for remove from DB trigger.db "{}" > "{}"'.format( str(e), str(pdata['pistributer']) ) )
            return False
        
        if not os.path.isfile( dbpath ):
            logError( 'dbpath gone ? this should not happen, might be data polution or data missing happen. "{}"'.format(dbpath) )
            return False
        
        delete_query = '''DELETE FROM `{}` WHERE `ID`="{}";'''.format( tablename, rowid )
        print(delete_query, dbpath)
        error = None
        output = None
        try:
            connection = sqlite3.connect(dbpath)
            cursor = connection.cursor()
            cursor.execute(delete_query)
            connection.commit()
        
        except Exception as e:
            error = '"{}" > "{}"'.format(str(e), delete_query)
            logError('error happen during delete the database, beware of this message because this might potentially lead to data polution "{}"'.format( error ) )
            
        finally:
            try:
                connection.close()
            except:
                pass
        
        if error:
            raise Exception( error )
        else:
            return output
        
    def isEmpty( self ):
        if self.__index['index'] >= len(self.q):
            try:
                os.remove( self.path['index'] )
                
            except:
                pass
            
            try:
                os.remove( self.path['in_use'] )
                
            except:
                pass
            
            if os.path.isfile( self.path['data'] ):
                self.__initialQueue( )
                return False
            
            return True
        
        else:
            return False
        
    def size( self ):
        paths = self.path['data'],self.path['in_use']
        size = 0
        for filepath in paths:
            if os.path.isfile( filepath ):
                with open(filepath) as fp:
                    for _ in fp:
                       size += 1
        return size
    
    def remaining( self ):
        return self.size() - self.getIndex()['index']
        
    def sqlconn(self, path):
        if not path.endswith('.db'):
            raise Exception( 'Path must ends with ".db" "{}"'.format(path) )
        self.sqlindex__ = 1
        self.sqllist = []
        create_table_query = '''CREATE TABLE IF NOT EXISTS Pistributer ( id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT )'''
        
        self.sqlconn = sqlite3.connect( path )
        self.sqlcursor = self.sqlconn.cursor()
        self.sqlcursor.execute( create_table_query )
        self.sqlconn.commit()
        
    def sqlcount(self):
        self.sqlopen()
        query = '''select seq from sqlite_sequence where name="Pistributer"'''
        query = '''select count(*) from Pistributer'''
        self.sqlopen()
        self.sqlcursor.execute(query)
        output = self.sqlcursor.fetchone()
        if output:
            return output[0]
        else:
            return 0
        
    def sqlput(self, data, flush=True):
        self.sqllist.append(str(data))
        if flush == True:
            self.sqlflush()
            
    def sqlputs(self, data, flush=True):
        self.sqllist.extend([str(i) for i in data])
        if flush == True:
            self.sqlflush()
        
    def sqlflush(self):
        try:
            self.sqlopen()
            while True:
                try:
                    data = self.sqllist.pop(0)
                    self.sqlcursor.execute(''' INSERT INTO Pistributer VALUES( NULL, ? ) ''', (data,) )
                except:
                    break
            self.sqlconn.commit()
        except Exception as e:
            print(str(e))
            
    def sqlindex(self):
        query = '''select seq from sqlite_sequence where name="Pistributer_channel" '''
        self.sqlopen()
        self.sqlcursor.execute(query)
        output = self.sqlcursor.fetchone()
        if not output:
            self.sqlcursor.execute('''UPDATE sqlite_sequence SET name="Pistributer_channel", seq=1''')
            self.sqlconn.commit()
            self.sqlindex__ = 1
            return self.sqlindex__
        else:
            self.sqlindex__ = output[0]
        return self.sqlindex__
    
    def sqlsize(self):
        return self.sqlcount()
    
    def sqlupdateindex(self):
        nextindex = self.sqlindex() + 1
        self.sqlopen()
        self.sqlcursor.execute( ''' UPDATE sqlite_sequence SET name="Pistributer_channel", seq=? ''', (nextindex,))
        self.sqlconn.commit()
    
    def sqlnext(self):
        index = self.sqlindex()
        query = '''SELECT channel FROM Pistributer WHERE ID = ?'''
        self.sqlopen()
        self.sqlcursor.execute( query, (index,) )
        output = self.sqlcursor.fetchone()
        if output:
            self.sqlupdateindex()
            return output[0]
        else:
            if not self.sqlisEmpty():
                return self.sqlnext()
            
    def sqlisEmpty(self):
        index = self.sqlindex()
        query = '''SELECT id FROM Pistributer WHERE ID = ?'''
        self.sqlopen()
        self.sqlcursor.execute( query, (index,) )
        output = self.sqlcursor.fetchone()
        if output:
            return False
        else:
            self.sqlcursor.execute( '''DELETE FROM Pistributer;''' )
            self.sqlcursor.execute('''UPDATE sqlite_sequence SET name="Pistributer_channel", seq=1''')
            self.sqlconn.commit()
            return True
    
    def sqlscan(self):
        index = self.sqlindex()
        query = '''SELECT channel FROM Pistributer WHERE ID = ?'''
        self.sqlopen()
        self.sqlcursor.execute( query, (index,) )
        output = self.sqlcursor.fetchone()
        if output:
            self.sqlupdateindex()
            return output[0]
        else:
            if not self.sqlisLast():
                return self.sqlscan()
            
    def sqlisLast(self):
        index = self.sqlindex()
        query = '''SELECT id FROM Pistributer WHERE ID = ?'''
        self.sqlopen()
        self.sqlcursor.execute( query, (index,) )
        output = self.sqlcursor.fetchone()
        if output:
            return False
        else:
            self.sqlcursor.execute('''UPDATE sqlite_sequence SET name="Pistributer_channel", seq=1''')
            self.sqlconn.commit()
            return True
        
    def sqlopen(self):
        try:
            self.sqlconn.open()
            self.sqlcursor = self.sqlconn.cursor()
        except:
            pass
        
    def sqlexit(self):
        try:
            self.sqlflush()
        except:
            pass
        try:
            self.sqlconn.close()
        except:
            pass
        
    def sqlclose(self):
        self.sqlexit()
    
class PistributerKv(Pistributer):
    
    def sqlkvconn(self, path):
        if not path.endswith('.db'):
            raise Exception( 'Path must ends with ".db" "{}"'.format(path) )
        self.sqlindex__ = 1
        self.sqllist = []
        create_table_query = '''CREATE TABLE IF NOT EXISTS PistributerKv ( id STRING PRIMARY KEY, channel TEXT )'''
        
        self.sqlconn = sqlite3.connect( path )
        self.sqlcursor = self.sqlconn.cursor()
        self.sqlcursor.execute( create_table_query )
        self.sqlconn.commit()
        
    def sqlkvcount(self):
        self.sqlopen()
        query = '''select seq from sqlite_sequence where name="PistributerKv"'''
        query = '''select count(*) from PistributerKv'''
        self.sqlopen()
        self.sqlcursor.execute(query)
        output = self.sqlcursor.fetchone()
        if output:
            return output[0]
        else:
            return 0
        
    def sqlkvset(self, key, value, flush=True):
        self.sqllist.append((str(key), str(value)))
        if flush == True:
            self.sqlkvflush()
            
    def sqlkvsetmany(self, data, flush=True):
        if isinstance(data, dict):
            self.sqllist.extend([(str(k), str(v)) for k, v in data.items()])
        else:
            self.sqllist.extend(data)
            
        if flush == True:
            self.sqlkvflush()
        
    def sqlkvunique(self, key, value, flush=True):
        """Do not overwrite when the key already exists.
        """
        try:
            self.sqlopen()
            try:
                self.sqlcursor.execute(''' INSERT INTO PistributerKv VALUES( ?, ? ) ''', (key, value) )
                if flush == True:
                    self.sqlkvuniqueflush()
            except Exception as e:
                print('"{}" key is already exists > {}'.format(key, str(e)))
        except Exception as e:
            print(str(e))
        
    def sqlkvuniqueflush(self):
        try:
            self.sqlconn.commit()
        except Exception as e:
            print(str(e))
        
    def sqlkvflush(self):
        try:
            self.sqlopen()
            while True:
                try:
                    data = self.sqllist.pop(0)
                    try:
                        self.sqlcursor.execute(''' INSERT INTO PistributerKv VALUES( ?, ? ) ''', data )
                    except:
                        self.sqlcursor.execute(''' UPDATE PistributerKv SET channel = ? WHERE id = ?''', (data[1], data[0]))
                except:
                    break
            self.sqlconn.commit()
        except Exception as e:
            print(str(e))
    
    def sqlkvsize(self):
        return self.sqlkvcount()
    
    def sqlkvget(self, key):
        query = '''SELECT channel FROM PistributerKv WHERE ID = ?'''
        self.sqlopen()
        self.sqlcursor.execute( query, (key,) )
        output = self.sqlcursor.fetchone()
        if output:
            return output[0]
    
    def sqlkvgetall(self, key):
        query = '''SELECT channel FROM PistributerKv'''
        self.sqlopen()
        self.sqlcursor.execute( query )
        output = self.sqlcursor.fetchall()
        if output:
            return output
    
    def sqlkvkeys(self):
        query = '''SELECT id FROM PistributerKv'''
        self.sqlopen()
        self.sqlcursor.execute( query )
        output = self.sqlcursor.fetchall()
        if output:
            return [ i[0] for i in output ]
        else:
            return []
        
    def sqlkvclose(self):
        self.sqlexit()