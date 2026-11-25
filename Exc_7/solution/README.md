Note: I probably ran the generate data twice which is why I have 4 instead of 2 million data samples

## Part 1 

1. **Run the example** (section 1 of the notebook). It copies the whole topic
   twice. Why does the checker fail?

The checker fails because we insert duplicates into our data lake. Every Partition and offset now appear twice. We have 4 million records but 8 million rows.

## Part 2

### My decision

I decide to go with architecture B. 

Simply because Architecture A seems error prone if the job executes and no state gets written to the file due to a crash of the system. Architecture C may add overhead because readers need to remove duplicate records whenever they query the data lake.

### Result explained

The result shows that all 5 tests passed. The lake had no missing or duplicate records.

**Run 1:**
All 4 million rows were successfully copied to the lake.

**Run 2:**
Added another 100 thousand records to the lake making it 4.1 million

**Run 3:**
Another run left the lake at 4.1 million records, without creating duplicates.

**Run 4:**
The 100,000 new records were not yet visible in the lake. The next run loaded them successfully.

**Run 5:**
The newly written records were still in staging and had not been published. The next run loaded them successfully.

This shows that the implementation preserves exactly-once behavior in the tested situations.

### Output

reset: lake, staging and state deleted

situation                                 in lake  duplicates  missing  not loaded    run  check
-----------------------------------------------------------------------------------------------
1 first run                             4,000,000           0        0           0  41.5s  PASS
2 new records arrive, run again         4,100,000           0        0           0   4.8s  PASS
3 nothing new, run again                4,100,000           0        0           0   0.1s  PASS
4 run stops WHILE writing: right after  4,100,000           0        0     100,000         PASS
  ... then the next run                 4,200,000           0        0           0   4.3s  PASS
5 run stops AFTER writing: right after  4,200,000           0        0     100,000         PASS
  ... then the next run                 4,300,000           0        0           0   3.9s  PASS
-----------------------------------------------------------------------------------------------
Exactly once after every run: YES
[('1 first run',
  {'topic': 4000000,
   'lake': 4000000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 4,
   'mb': 222.922233,
   'ok': True,
   'problems': []},
  41.500938415527344),
 ('2 new records arrive, run again',
  {'topic': 4100000,
   'lake': 4100000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 8,
   'mb': 224.081961,
   'ok': True,
   'problems': []},
  4.80363917350769),
 ('3 nothing new, run again',
  {'topic': 4100000,
   'lake': 4100000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 8,
   'mb': 224.081961,
   'ok': True,
   'problems': []},
  0.059888362884521484),
 ('4 run stops WHILE writing: right after',
  {'topic': 4200000,
   'lake': 4100000,
   'dup': 0,
   'miss': 0,
   'lag': 100000,
   'files': 8,
   'mb': 224.081961,
   'ok': True,
   'problems': []},
  None),
 ('  ... then the next run',
  {'topic': 4200000,
   'lake': 4200000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 12,
   'mb': 225.240031,
   'ok': True,
   'problems': []},
  4.27383017539978),
 ('5 run stops AFTER writing: right after',
  {'topic': 4300000,
   'lake': 4200000,
   'dup': 0,
   'miss': 0,
   'lag': 100000,
   'files': 12,
   'mb': 225.240031,
   'ok': True,
   'problems': []},
  None),
 ('  ... then the next run',
  {'topic': 4300000,
   'lake': 4300000,
   'dup': 0,
   'miss': 0,
   'lag': 0,
   'files': 16,
   'mb': 226.292718,
   'ok': True,
   'problems': []},
  3.8771779537200928)]