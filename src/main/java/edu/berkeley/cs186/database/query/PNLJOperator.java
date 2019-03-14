package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.Database;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.io.PageAllocator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;

public class PNLJOperator extends JoinOperator {
    public PNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException, DatabaseException {
        super(leftSource,
              rightSource,
              leftColumnName,
              rightColumnName,
              transaction,
              JoinType.PNLJ);

        // for HW4
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        return new PNLJIterator();
    }

    public int estimateIOCost() throws QueryPlanException {
        //does nothing
        return 0;
    }

    /**
     * PNLJ: Page Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might prove to be a useful reference).
     */
    private class PNLJIterator extends JoinIterator {
        /**
         * Some member variables are provided for guidance, but there are many possible solutions.
         * You should implement the solution that's best for you, using any member variables you need.
         * You're free to use these member variables, but you're not obligated to.
         */

        private BacktrackingIterator<Page> leftIterator = null;
        private BacktrackingIterator<Page> rightIterator = null;
        private BacktrackingIterator<Record> leftRecordIterator = null;
        private BacktrackingIterator<Record> rightRecordIterator = null;
        private Record leftRecord = null;
        private Record rightRecord = null;
        private Record nextRecord = null;

        public PNLJIterator() throws QueryPlanException, DatabaseException {
            super();
            this.leftIterator = getTransaction().getPageIterator(this.getLeftTableName());
            this.rightIterator = getTransaction().getPageIterator(this.getRightTableName());
            this.leftIterator.next();
            this.rightIterator.next();
            this.leftRecordIterator = PNLJOperator.this.getBlockIterator(this.getLeftTableName(), this.leftIterator, 1);
            this.rightRecordIterator = PNLJOperator.this.getBlockIterator(this.getRightTableName(), this.rightIterator, 1);
            rightIterator.mark();
            this.leftRecord = this.leftRecordIterator.hasNext() ? this.leftRecordIterator.next() : null;
            this.rightRecord = this.rightRecordIterator.hasNext() ? this.rightRecordIterator.next() : null;
            if (this.leftRecord != null) {
                this.leftRecordIterator.mark();
            } else {
                return;
            }
            if (this.rightRecord != null) {
                this.rightRecordIterator.mark();
            }
            fetchNextRecord();
        }

        private void fetchNextRecord() throws DatabaseException{
            if (this.leftRecord == null) {throw new DatabaseException("No new record to fetch");}
            this.nextRecord = null;
            do {
                if (this.rightRecord != null) {
                    DataBox leftJoinValue = this.leftRecord.getValues().get(PNLJOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(PNLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                        List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                    }
                    this.rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                } else {
                    if (this.leftRecordIterator.hasNext()) {
                        rightRecordIterator.reset();
                        this.leftRecord = leftRecordIterator.next();
                        this.rightRecord = rightRecordIterator.next();
                    } else {
                        try {
                            fetchNextRightPage();
                            this.rightRecord = rightRecordIterator.next();
                            this.rightRecordIterator.mark();
                            leftRecordIterator.reset();
                            this.leftRecord = leftRecordIterator.next();
                        } catch (DatabaseException e) {
                            rightIterator.reset();
                            rightRecordIterator = PNLJOperator.this.getBlockIterator(this.getRightTableName(), this.rightIterator, 1);
                            this.rightRecord = rightRecordIterator.next();
                            this.rightRecordIterator.mark();
                            try {
                                fetchNextLeftPage();
                                this.leftRecord = leftRecordIterator.next();
                                this.leftRecordIterator.mark();
                            } catch (DatabaseException t) {
                                this.nextRecord = null;
                                return;
                            }
                        }
                    }
                }
            } while (!hasNext());
        }

        public void fetchNextRightPage() throws DatabaseException{
            if (rightIterator.hasNext()) {
                rightRecordIterator = PNLJOperator.this.getBlockIterator(this.getRightTableName(), this.rightIterator, 1);
            } else {
                throw new DatabaseException("No more right pages.");
            }
        }

        public void fetchNextLeftPage() throws DatabaseException{
            if (leftIterator.hasNext()) {
                leftRecordIterator = PNLJOperator.this.getBlockIterator(this.getLeftTableName(), this.leftIterator, 1);
            } else {
                throw new DatabaseException("No more left pages.");
            }
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
            if (!hasNext()) {throw new NoSuchElementException();}
            Record nextRecord = this.nextRecord;
            try {
                fetchNextRecord();
            } catch (DatabaseException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

