import unittest,shutil,tempfile,os
from hfile import Item,Node
from hfile_exceptions import ObjectNotFound,ObjectMalformed

class Node1(Node):
    infos_fields=set(["test1","test2"])
    pass

class HNode1(Node):
    isChild=False
    childType=None
    infos_fields=set(["test1","test2"])
class Children_HNode(Node):
    isChild=True
    childType=None
    infos_fields=set(["test1","test2"])
class Father_HNode(Node):
    isChild=False
    childType=Children_HNode
    infos_fields=set(["test1","test2"])

class Item1(Item):
    infos_fields=set(["test1","test2"])
    pass
    
classes=[Node1,HNode1,Children_HNode,Father_HNode,Item,Item1]
for c in classes:
    c.initialize("/tmp/")

class test_Node1(unittest.TestCase):
    storageClass=Node1
    def setUp(self):
        self.__class__.storageClass.SaveDir=tempfile.mkdtemp()
        self.__class__.storageClass.infos_fields=set(["test1","test2"])
        self.create()
        
    def create(self):
        self.s=self.__class__.storageClass(data={"test1":"coucou","test2":[1,2,3]})

    def tearDown(self):
        shutil.rmtree(self.__class__.storageClass.SaveDir)

    def test_create(self):
        self.assertIsInstance(self.s, self.__class__.storageClass)
        self.assertTrue(os.path.exists(self.s._infos_path()))
        infos={"test1":"coucou","test2":[1,2,3],"id":self.s.id}
        self.assertEqual(self.s.infos,infos)

    def test_load(self):
        s=self.__class__.storageClass(id=self.s.id)
        self.assertEqual(self.s.infos,s.infos)
        self.assertEqual(self.s.id,s.id)
        
    def test_update(self):
        self.s.infos={"test1":1,"test2":2,"id":self.s.id}
        self.s.update()
        s=self.__class__.storageClass(id=self.s.id)
        self.assertEqual(self.s.infos,s.infos)

    def test_remove(self):
        self.s.remove()
        self.assertFalse(os.path.exists(self.s._infos_path()))
        self.assertRaises(ObjectNotFound, lambda :self.__class__.storageClass(id=self.s.id))

    def test_put_item(self):
        data={"test1":"coucou","test2":[1,2,3]}
        i=self.s.put_item(infos=data,cls_item=Item1)
        self.assertTrue(os.path.exists(i._infos_path()))
        
    def test_get_item(self):
        data={"test1":"coucou","test2":[1,2,3]}
        i=self.s.put_item(infos=data,cls_item=Item1)        
        self.assertEqual(i.infos,self.s.get_item(i.id,Item1).infos)

    def test_change_item(self):
        data={"test1":"coucou","test2":[1,2,3]}
        i=self.s.put_item(infos=data,cls_item=Item1)
        data2={"test1":"comment","test2":"va?","id":i.id}
        self.s.change_item(i.id,Item1,data2)
        self.assertEqual(data2,self.s.get_item(i.id,Item1).infos)
        
    def test_remove_item(self):
        data={"test1":"coucou","test2":[1,2,3]}
        i=self.s.put_item(infos=data,cls_item=Item1)
        self.s.remove_item(i.id,Item1)
        self.assertRaises(ObjectNotFound, lambda :self.s.get_item(i.id,Item1))
        
    def test_list_items(self):
        items=[]
        for i in range(0,10):
            items.append(self.s.put_item(infos={},cls_item=Item).infos)
        litems,total=self.s.list_items(Item)
        litems=[s.infos for s in litems]
        self.assertEqual(sorted(litems),sorted(items))
        self.assertEqual(total,10)
    
    def test_list_all_items(self):
        items=[]
        for i in range(0,5):
            items.append(self.s.put_item(infos={},cls_item=Item).infos)
        data={"test1":"coucou","test2":[1,2,3]}    
        for i in range(0,5):
            items.append(self.s.put_item(infos=data,cls_item=Item1).infos)

        
class test_HNode1(test_Node1):
    storageClass=HNode1
    def test_get_childs(self):
        self.assertRaises(AttributeError,self.s.get_childs)
        
class test_Children_HNode(test_HNode1):
    storageClass=Children_HNode

    def setUp(self):
        Father_HNode.SaveDir=tempfile.mkdtemp()
        self.father=Father_HNode(data={"test1":"coucou","test2":[1,2,3]})
        super(test_Children_HNode, self).setUp()
        
    def create(self):
        data={"test1":"coucou","test2":[1,2,3]}
        self.s=Children_HNode(data=data,parent=self.father)      

    def test_load(self):
        s=self.storageClass(id=self.s.id,parent=self.father)
        self.assertEqual(self.s.infos,s.infos)
        self.assertEqual(self.s.id,s.id)
        
    def test_update(self):
        self.s.infos={"test1":1,"test2":2,"id":self.s.id}
        self.s.update()
        s=self.storageClass(id=self.s.id,parent=self.father)
        self.assertEqual(self.s.infos,s.infos)

    def test_remove(self):
        self.s.remove()
        self.assertFalse(os.path.exists(self.s._infos_path()))
        self.assertRaises(ObjectNotFound, lambda :self.storageClass(id=self.s.id,parent=self.father))
        
class test_Father_HNode(test_Node1):
    storageClass=Father_HNode
    def setUp(self):
        super(test_Father_HNode, self).setUp()
    
    def test_get_childs(self):
        children=[]
        for i in range(0,20):
            children.append(Children_HNode(data={"test1":"coucou","test2":[1,2,3]},parent=self.s))
            
        self.assertEqual(sorted([s.infos for s in children]),sorted([s.infos for s in self.s.get_childs(0,0)[0]]))
        l=[s.infos for s in self.s.get_childs(0,5)[0]]
        for s in l:
            self.assertEqual(len(l),5)
            self.assertIn(s, [s.infos for s in children])
            

        

if __name__ == '__main__':
    unittest.main()
