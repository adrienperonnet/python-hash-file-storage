#python-hash-file-storage

A small script to easely store your datas inside small raw text files, with hashed name.

Final goal: easely synchronize your data with a revision control tool.

##Example:

###Possible use case
Let's imagine you want to syncronize user data between to python application on differents servers.
These two server could have sporadic connections.
User data can be add/delete/change on each server.
Sometimes, you will need to synchronize the two databases "user".
With this app, you can do it with git, since each user is stored in a separated file, with a unique hashed name.

###Usage
```python
from hfile import Node

#We create our data model
class User(Node):
    infos_fields=set(("name","age"))

#Let's store our data in /tmp/User directory
User.initialize("/tmp/")

#Save some users
User(data={"name":"Marc","age":10})
>>>User_id1
id2=User(data={"name":"Henry","age":33})
>>>User_id2

#List user in Database
User.get_currents_obj()
>>>[id1,id2],2

#Remove data
User(id=id1).remove()

User.get_currents_obj()
>>>[id2],1

```
