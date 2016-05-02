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
		
6. .

		db.test.mapReduce(
			function(){
				if (this.awards){
					for (var i=0;i<this.awards.length;i++){
						emit(this.awards[i].award,1);
					}
				}
			},
			function(key,values){
					return Array.sum(values)
			},
			{out:"Q6"}
		)
		db.Q6.find()
		
7. .
		
		db.test.aggregate([ {$match:{}}, 
								{$group: {_id: 
												{$cond:[{$ifNull:["$birth",0]}, 
												        {$year:"$birth"}, 
												        -1]},
											ids:{$push:"$_id"}
										}}])
										
8. .

		db.test.aggregate([{$match:{}},{$group:{_id:0,max:{$max:"$_id"},min:{$min:"$_id"}}}])
		
9. .

		db.test.createIndex( { "$**": "text" } )
		db.test.find({$text:{$search:"\"Turing award\""}})
		
10. .  

		function exclude(arr,obj) {return (arr.indexOf(obj) < 0);}
		var a = db.test.find( {$text:{$search:"Turing"}})
		var b = db.test.find( {$text:{$search:"\"National Medal\""}})
		res = []
		key= []
		while (a.hasNext()){t = a.next();res.push(t);key.push(t._id.valueOf());}
		while (b.hasNext()){t = b.next();if (exclude(key,t._id.valueOf())) res.push(t);}
		res
