#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author:       Adrien Péronnet
@contact:      adrien@apapa.fr
"""

import datetime,os,json,glob,logging,time,shutil,uuid,threading
from utils import opened_w_error,memoize
from hfile_exceptions import ObjectNotFound,ObjectMalformed


"""
Basic common methods used to store data in hashed files
"""
class HFile(object):
    """
    This method is used to generate the name of the directories containing the datas.
    We need to choose a hash with the maximum entropy to avoid collision and problematics merges.
    """

    """
    We instaure a hierarchy between the datas
    """
    isChild=False
    childType=None

    """
    Make all the methods in this class Thread Safe
    """
    _Locks={} # This dictionary contain the lock on each file. And male the reading/editing of each file thread safe.
    _Lock=threading.Lock() # This lock make the Locks dictionary thread safe.
    
    """
    Name of the keys of the self.info dictionnary.
    Put here all the name of the variable you will need to be stored with the object.
    """
    infos_fields=set([])

    """
    Where to store the data on disk ?
    if a data file is: /home/$USER/data/objects/hash1/infos.json
    we have:
        SavePath=/home/$USER/data
        Dir=objects
        _get_path()=        /home/$USER/data/objects/
        _get_object_path()= /home/$USER/data/objects/hash1
        _infos_path()=      /home/$USER/data/objects/hash1/infos.json
    """
    SavePath=None #The path to the directory Dir
    Dir=None #The name of the directory containing all the datas.

    @classmethod
    def _get_path(cls):
        return "%s/%s"%(cls.SavePath,cls.Dir)

    def _get_object_path(self):
        if self.__class__.isChild:
            return self.parent._get_object_path()

    def _infos_path(self):
        raise NotImplementedError

    @classmethod
    def initialize(cls,SavePath):
        """
        This method take care of creating the initial directory for the data objects
        """
        cls.Dir=cls.__name__
        cls.SavePath=SavePath
        if  not os.path.exists(cls._get_path()):
            os.mkdir(cls._get_path())
 

    """
    We check the parameter passed to the dictionary before saving the object.
    """
    def _check_valid_dict(self, infos=None):
        if infos is None:
            infos = self.infos
        if not isinstance(infos, dict):
			raise ObjectMalfomered(data_object=self,message="You should pass a dictionary to initiate the object")
        i_set=set([k for k,_ in infos.items() if k != "id"])
        if i_set != self.__class__.infos_fields:
            raise ObjectMalformed(data_object=self,message="Corrupted data ('%s' are not allowed and '%s' are required)"%(
                ', '.join(i_set.difference(self.__class__.infos_fields)),
                ', '.join((self.__class__.infos_fields).difference(set(i_set)))
                ))

    
    def __init__(self,parent=None,data=None,id=None,load=True):
        """
        Write or read data in a file
        >obj=Obj(data={"test":"t"})
        recover the object from storage, with specified id
        >obj=Obj(id=5,load=True) 
        recover the object from storage, with specified id without reading the file: only to use specifics methods like remove().
        >obj=Obj(id=5,load=False)
        """
        #Cache the function result
        if self.__class__.isChild and parent is None:
            raise AttributeError("The class %s is a child, you should initialize it with a parent instance."%self.__class__)
        if data is not None and id is not None:
            raise AttributeError("Ambiguous constructor for class %s"%self.__class__)
        if self.__class__.isChild:
            self.parent=parent
        if data is not None:
            self._new_id() 
            self.infos=data
            self.update() 
        elif id is not None:
            self.id=id
            if self.id not in self.__class__._Locks:
                with self.__class__._Lock:
                    self.__class__._Locks[self.id]=threading.Lock()
            if load:
                self.infos=self._get()
        else:
            raise AttributeError("Invalid constructor for class %s"%self.__class__)
        #We want to easily recuperate the substantial data in self.infos
        if 	load:
            self.infos["id"]=self.id
            self._check_valid_dict()
        
    @memoize
    def _get(self):
        """
        Read data from the file.
        """
        with self.__class__._Locks[self.id]:
            try:
                with opened_w_error(self._infos_path(),"r") as f:
                    return json.load(f)
            except IOError as e:
                raise ObjectNotFound(data_object=self,message="Can't open the file",original_message=e)
            except ValueError as e:
                raise ObjectMalformed(data_object=self,message="Invalid Json Document",original_message=e)

    def _new_id(self):
        """Set a new non-used hash id to the object"""
        self.id=HFile._generate_id()
        while os.path.exists(self._infos_path()):
            self.id=HFile._generate_id()
        with self.__class__._Lock:
            self.__class__._Locks[self.id]=threading.Lock()
        
        
    def update(self):
        self._check_valid_dict()
        """Save the new data in the data file"""
        with self.__class__._Locks[self.id]:
            with opened_w_error(self._infos_path(),"w") as f:
                json.dump({k: value for k,value in self.infos.items() if k != "id"},f, indent=4)
            try:
                with opened_w_error(self._infos_path(),"w") as f:
                    #We don't store the id information since it's just a part of the file path.
                    json.dump({k: value for k,value in self.infos.items() if k != "id"},f, indent=4)
            except IOError as e:
                raise ObjectNotFound(data_object=self,message="Can't save data.",original_message=e)
            except (ValueError,TypeError) as e:
                raise ObjectMalformed(data_object=self,message="Can't serialize data.",original_message=e)

            #Also update the cache of self.__get
            self._get.__func__.cache[(self,)]=self.infos

    def remove(self):
        """Entirely remove the data file object"""
        with self.__class__._Locks[self.id]:
            try:
                os.remove(self._infos_path())
            except OSError as e:
                raise ObjectNotFound(data_object=self,message="Can't remove the data",original_message=e)
        del self.__class__._Locks[self.id]
        del self._get.__func__.cache[(self,)]


    @staticmethod
    def _generate_id():
        """
        Generate a random ID.
        We store the time in the beginning of the filename to be able to quickly get the last added data.
        """
        return uuid.uuid4().hex
   
    def __str__(self):
        try:
            return "%s_%s"%(self.__class__.__name__,self.id) 
        except:
            return "Object %s"%self.__class__

    def __repr__(self):
        try:
            return "%s_%s"%(self.__class__.__name__,self.id)
        except:
            return self.__class__.__name__

    def __hash__(self):
        return uuid.UUID(hex=self.id).int

    def __eq__(self,object):
        """Two objects are identicals if they represent the same data on the disk"""
        return isinstance(object,self.__class__) and self.id==object.id


class Item(HFile):
    """
    An Items is a hashed JSON file containing basic data.
    An Item is used to stored additional data, it is attached to a parent Node.
    Items can be grouped in different type.
    A node can contains different Items of different types
    """  
 
    isChild=True
    childType=None

    def _infos_path(self):
        return "%s/%s__%s.item"%(self._get_object_path(),self.id,self.__class__.__name__)



class Node(HFile): 
    """
    A Node is a hashed directory containing a simple JSON containing basic data.
    A Node can contains multiple Items inside its directory.
    """
    
    @staticmethod
    def _get_filenames(path,begin=0,end=0):
        """Get the filenames (without extension) inside the specified path"""
        #return all the filenames inside the path
        
        if begin==end==0:
            fnames=map(os.path.basename, glob.glob(path))
        else:
            fnames=map(os.path.basename, glob.glob(path))[begin:end]
        #We get rid of file extensions.
        return map(lambda x:os.path.splitext(x)[0],fnames),len(fnames)
        
           
    @classmethod
    def get_currents_obj(cls,begin=0,end=100): 
        """Get all the objects inside the main directory"""
        fnames,total=cls._get_filenames("%s/*.obj"%cls._get_path(),begin,end)
        return [cls(id=name) for name in fnames],total
            
    def list_items(self,cls_item,begin=0,end=100):
        """Get all the items depending of the specified object"""
        fnames,total=self._get_filenames("%s/*__%s.item"%(self._get_object_path(),cls_item.__name__),begin,end)
        #We parse the id from the file name
        ids=map(lambda name:name.split("__")[0],fnames)
        return [cls_item(id=id,parent=self) for id in ids],total

    def get_childs(self,begin=0,end=100):
        """Get all the child of the specified object"""
        if self.__class__.childType==None:
            raise AttributeError("This is as parent class, cant't call a get_childs method")
        fnames,total=self._get_filenames("%s/*.obj"%self._get_object_path(),begin,end)
        return [self.__class__.childType(id=name,parent=self) for name in fnames],total

    def _new_id(self):
        super(Node,self)._new_id()
        #Each object has his own directory.
        os.mkdir(self._get_object_path())

    def put_item(self,infos,cls_item):
        """store a new item inside this node"""
        return cls_item(parent=self,data=infos)
        
    def get_item(self,iditem,cls_item):
        """Extract an existing item inside this node"""
        return cls_item(parent=self,id=iditem)

    def change_item(self,iditem,cls_item,infos):
        """Modify the specified item"""
        i=cls_item(parent=self,id=iditem,load=False)
        i.infos=infos
        i.update()
        return i
    
    def remove_item(self,iditem,cls_item):
        """Remove the specified item inside this node"""
        return cls_item(parent=self,id=iditem,load=False).remove()
        
    def _infos_path(self):
        return "%s/infos.json"%self._get_object_path() 
    
    def _get_object_path(self):
        dir=self.parent._get_object_path() if self.__class__.isChild else self.__class__._get_path()
        return "%s/%s.obj"%(dir,self.id)

    def remove(self):
        with self.__class__._Locks[self.id]:
            try:
                shutil.rmtree(self._get_object_path())
            except OSError as e:
                raise ObjectNotFound(data_object=self,message="Can't remove the data",original_message=e)
        del self.__class__._Locks[self.id]
