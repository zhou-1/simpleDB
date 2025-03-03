package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;
    private Map<Field,List<Tuple>> map;
    
    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    
    private Tuple t1;
    private Tuple t2;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
    	this.p = p;
    	this.child1 = child1;
    	this.child2 = child2;
    	this.map = new HashMap<Field,List<Tuple>>();
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
    	return this.p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }
    
    public String getJoinField1Name()
    {
        // some code goes here
    	return this.child1.getTupleDesc().getFieldName(this.p.getField1());
    }

    public String getJoinField2Name()
    {
        // some code goes here
    	return this.child2.getTupleDesc().getFieldName(this.p.getField2());
    }
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	child1.open();
    	child2.open();
    	loadMap();
    	super.open();
    }
    
    private boolean loadMap() throws DbException, TransactionAbortedException{
    	int cout = 0;
    	map.clear();
    	while(child1.hasNext()) {
    		t1 = child1.next();
    		List<Tuple> list = map.get(t1.getField(p.getField1()));
    		if (list == null) {
    			list = new ArrayList<Tuple>();
    			map.put(t1.getField(p.getField1()), list);
    		}
    		list.add(t1);
    		if (cout++ == 20000) return true;
    	}
    	return cout > 0;
    }

    public void close() {
        // some code goes here
    	super.close();
    	child1.close();
    	child2.close();
    	t1 = null;
    	t2 = null;
    	map.clear();
    	listIt = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child1.rewind();
    	child2.rewind();
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (listIt != null && listIt.hasNext()) {
    		return joinTuples();
    	}
        while (child2.hasNext()) {
        	t2 = child2.next();
        	List<Tuple> list = map.get(t2.getField(p.getField2()));
        	if (list != null) {
            	listIt = list.iterator();
            	return joinTuples();
        	} 
        }
        child2.rewind();
        if (loadMap()) {
        	return fetchNext();
        }
        return null;
    }
    
    private Tuple joinTuples() throws TransactionAbortedException, DbException{
    	t1 = listIt.next();
    	int size1 = t1.getTupleDesc().numFields();
    	int size2 = t2.getTupleDesc().numFields();
    	
    	Tuple mergeT = new Tuple(this.getTupleDesc());
    	for (int i = 0; i < size1; i++) {
    		mergeT.setField(i, t1.getField(i));
    	}
    	for (int i = 0; i < size2; i++) {
    		mergeT.setField(i+size1, t2.getField(i));
    	}
    	return mergeT;
    }
    

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	return new DbIterator[] {child1,child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	child1 = children[0];
    	child2 = children[1];
    }
    
}
