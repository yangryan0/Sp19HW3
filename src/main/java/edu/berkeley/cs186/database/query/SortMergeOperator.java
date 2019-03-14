package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;

public class SortMergeOperator extends JoinOperator {
    public SortMergeOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        // for HW4
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new SortMergeIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */

        private String leftTableName;
        private String rightTableName;
        private RecordIterator leftIterator;
        private RecordIterator rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;
        private LR_RecordComparator LRComparator;

        public SortMergeIterator() throws QueryPlanException, DatabaseException {
            super();
            SortOperator sort_left = new SortOperator(getTransaction(), getLeftTableName(), new LeftRecordComparator());
            SortOperator sort_right = new SortOperator(getTransaction(), getRightTableName(), new RightRecordComparator());
            this.leftTableName = sort_left.sort();
            this.rightTableName = sort_right.sort();
            this.leftIterator = getRecordIterator(this.leftTableName);
            this.rightIterator = getRecordIterator(this.rightTableName);
            this.leftRecord = this.leftIterator.hasNext() ? this.leftIterator.next() : null;
            this.rightRecord = this.rightIterator.hasNext() ? this.rightIterator.next() : null;
            this.nextRecord = null;
            this.marked = false;
            this.LRComparator = new LR_RecordComparator();
            try {
                fetchNextRecord();
            } catch (Exception e) {
                return;
            }

        }

        private void fetchNextRecord() throws DatabaseException{
            if (this.leftRecord == null) {throw new DatabaseException("out of left pages");}
            this.nextRecord = null;
            do {
                if (!marked) {
                    while (this.LRComparator.compare(this.leftRecord, this.rightRecord) < 0) {
                        this.leftRecord = this.leftIterator.hasNext() ? this.leftIterator.next() : null;
                    }
                    while (this.LRComparator.compare(this.leftRecord, this.rightRecord) > 0) {
                        this.rightRecord = this.rightIterator.hasNext() ? this.rightIterator.next() : null;
                    }
                    marked = true;
                    this.rightIterator.mark();
                }
                if (this.LRComparator.compare(this.leftRecord, this.rightRecord) == 0) {
                    DataBox leftJoinValue = this.leftRecord.getValues().get(getLeftColumnIndex());
                    DataBox rightJoinValue = this.rightRecord.getValues().get(getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                        List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                    }
                    this.rightRecord = this.rightIterator.hasNext() ? this.rightIterator.next() : null;
                } else {
                    this.rightIterator.reset();
                    this.rightRecord = this.rightIterator.hasNext() ? this.rightIterator.next() : null;
                    this.leftRecord = this.rightIterator.hasNext() ? this.leftIterator.next() : null;
                    this.marked = false;
                }
            } while (!hasNext());
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public Record next() {
            if (!hasNext()) {throw new NoSuchElementException("No more elements");}
            Record record = this.nextRecord;
            try {
                fetchNextRecord();
            } catch (Exception e) {
                this.nextRecord = null;
            }
            return record;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }

        /**
        * Left-Right Record comparator
        * o1 : leftRecord
        * o2: rightRecord
        */
        private class LR_RecordComparator implements Comparator<Record> {
            public int compare(Record o1, Record o2) {
                if (o1 == null) {
                    return -1;
                }
                if (o2 == null) {
                    return 1;
                }
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
