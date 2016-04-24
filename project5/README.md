# CS561 project5
### Advanced Query in MongoDB
 Shile Zhao, Qian Wang

1. .  
    
		var key="MongoDB";
		var lv = 0;
		var arr = Array()
		while (true) {
    		lv++;
    		parent = db.tree.findOne({"_id":key}).parent;
    		if (parent==null) break;
    		arr.push({parent:tojson(parent),"level":lv});
    		key=parent
		}
		arr
2. .

		var root = ["Books"]
		height = function(parents){
				if (parents.length==0) return 0;
				children = [];
				for (i=0;i<parents.length;i++){
					var cur = db.tree.find({"parent":parents[i]});
					while(cur.hasNext())
						children.push(cur.next()._id);
				}
				return height(children)+1;
			}
		height(root)
3. .    

		db.tree2.findOne({"children":"dbm"})._i
4. .   

		var root = "Books"
		descendants = function(parent){
					var list =  db.tree2.findOne({_id:parent}).children
					var ret=[]
					if (list.length==0) return ret
					ret=list.slice()
					for(i=0;i<list.length;i++){
						ret = ret.concat(descendants(list[i]))
					}
					return ret
			}
		descendants(root)		
5. .

		db.tree2.findOne({"children":"Databases"}).children.pop("Database")