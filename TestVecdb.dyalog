:Namespace TestVecdb

    ⍝ Updated to version 0.2.0 with sharding
    ⍝ Call TestVecdb.RunAll to run a full system test 
    ⍝   assumes vecdb is loaded in #.vecdb
    ⍝   returns memory usage statistics (result of "memstats 0")
    
    (⎕IO ⎕ML)←1 1    
    LOG←1      
    
    ∇ z←RunAll;path;source
      ⎕FUNTIE ⎕FNUMS ⋄ ⎕NUNTIE ⎕NNUMS
      :Trap 6 ⋄ source←SALT_Data.SourceFile
      :Else ⋄ source←⎕WSID
      :EndTrap
      path←{(-⌊/(⌽⍵)⍳'\/')↓⍵}source
      ⎕←'Testing vecdb version ',#.vecdb.Version
      ⎕←Basic
      ⎕←Sharding
    ∇
   
    ∇ r←Sharding;columns;data;options;params;folder;types;name;db
     ⍝ Test database with 2 shards
     
      folder←path,'\',(name←'shardtest'),'\'
      ⎕←'Clearing: ',folder
      :Trap 22 ⋄ #.vecdb.Delete folder ⋄ :EndTrap
     
      columns←'Name' 'BlockSize'
      types←,¨'C' 'F'
      data←('IBM' 'AAPL' 'MSFT' 'GOOG' 'DYALOG')(160.97 112.6 47.21 531.23 999.99)
     
      options←⎕NS''
      options.BlockSize←10000
      options.ShardFolders←(folder,'Shard')∘,¨'12'
      options.(ShardFn ShardCols)←'{2-2|⎕UCS ⊃¨⊃⍵}' 1
     
      params←name folder columns types options(3↑¨data)
      db←⎕NEW #.vecdb params
      assert 3=db.Count
      assert(3↑¨data)≡db.Read(1 2⍴1(1 2 3))columns ⍝ All went into shard #1
     
      db.Append columns(3↓¨data)
      assert 5=db.Count
      ix←db.Query('Name'(1⊃data))⍬ ⍝ Should find everything
     
    ∇
     
    ∇ x←output x
      :If LOG ⋄ ⍞←x ⋄ :EndIf
    ∇
    
    ∇ r←fmtnum x
    ⍝ Nice formatting of large integers
      r←(↓((⍴x),20)⍴'CI20'⎕FMT⍪,x)~¨' '
    ∇
    
    ∇ r←memstats reset;maxws;z
      :If reset=1
          z←0(2000⌶)14 ⍝ Reset high water mark
      :Else
          maxws←⊂⍕2 ⎕NQ'.' 'GetEnvironment' 'MAXWS'
          r←⎕WA
          r←'MAXWS' '⎕WA' 'WS Used' 'Allocated' 'High Water Mark',⍪¯20↑¨maxws,fmtnum r,(2000⌶)1 13 14
      :EndIf
    ∇

    assert←{'Assertion failed'⎕SIGNAL(⍵=0)/11}  

      time←{⍺←⊣ ⋄ t←⎕AI[3]
          o←output TEST,' ... '
          z←⍺ ⍺⍺ ⍵
          o←output(⍕⎕AI[3]-t),'ms',⎕UCS 10
          z
      }
    
    ∇ z←Basic;columns;types;folder;name;db;tnms;data;numrecs;recs;select;where;expect;indices;options;params;range;rcols;rcoli;newvals;i;t;vals;ix
     ⍝ Create and delete some tables
     
      numrecs←5000000 ⍝ 5 million records
      memstats 1      ⍝ Clear memory statistics
     
      folder←path,'\',(name←'testdb1'),'\'
      ⎕←'Clearing: ',folder
      :Trap 22 ⋄ #.vecdb.Delete folder ⋄ :EndTrap
     
      ⎕←'Creating: ',folder←path,'\',(name←'testdb1'),'\'
      columns←'col_'∘,¨types←#.vecdb.TypeNames
      assert #.vecdb.TypeNames≡tnms←'I1' 'I2' 'I4',,¨'FBC' ⍝ Types have been added?
      range←2*¯1+8×1 2 4 6 0.25
      data←numrecs⍴¨¯1+⍳¨numrecs⌊range
      data←data×0.1*'F'=⊃¨(≢data)↑types ⍝ Make float values where necessary
      data←data,⊂numrecs⍴{1↓¨(⍵=⊃⍵)⊂⍵}'/zero/one/two/three/four/five/six/seven/eight/nine/ten/eleven/one dozen/thirteen/fourteen/fifteen'
     
      :If LOG ⋄ ⎕←'Size of input data: ',fmtnum ⎕SIZE'data' ⋄ :EndIf
     
      recs←numrecs(⌊÷)2
      (options←⎕NS'').BlockSize←numrecs(⌊×)0.6 ⍝ Provoke block overflow
      params←name folder columns types options(recs↑¨data)
      TEST←'Creating db & inserting ',(fmtnum recs),' records'
      db←⎕NEW time #.vecdb params
      assert db.isOpen
      assert db.Count=recs
      assert 0=db.Close
      assert 0=db.isOpen
     
      TEST←'Reopen database'
      db←⎕NEW time #.vecdb(,⊂folder) ⍝ Open it again
      assert db.isOpen
      assert db.Count=recs
      TEST←'Reading them back:'
      assert(recs↑¨data)≡db.Read time(⍳recs)columns
     
      ⍝ test vecdb.Append and vecdb.Read
      TEST←'Appending ',(fmtnum numrecs-recs),' more'
      assert 0=db.Append time columns(recs↓¨data)  ⍝ Append the rest of the data
      assert db.Count=numrecs
      assert data≡db.Read(⍳numrecs)columns    ⍝ Read and verify ALL the data
     
      ⍝ Test vecdb.Query
      select←⌽columns ⍝ columns to select (all, but in reverse order)
      where←((1⊃columns)(1 2 3))
      expect←⌽((1⊃data)∊1 2 3)∘/¨data         ⍝ The expected result
      TEST←'Single expression query'
      assert expect≡db.Query time where select
     
      where←where((6⊃columns)(vals←'one' 'two' 'three' 'seventy')) ⍝ Add filter on char type
      expect←⌽(⊃∧/data[1 6]∊¨(1 2 3)vals)∘/¨data                 ⍝ Reduced expectations
      TEST←'Two expression query'
      assert expect≡db.Query time where select
     
      ⍝ Test vecdb.Replace
      indices←db.Query where ⍬
      rcols←columns[rcoli←types⍳,¨'I2' 'B' 'C']
      TEST←'Updating ',(fmtnum≢ix←2⊃,indices),' records'
      newvals←0 1-(⊂ix)∘⌷¨data[2↑rcoli] ⍝ Update with 0-data or ~data
      newvals,←⊂(≢ix)⍴⊂'changed'        ⍝ And new char values
      assert 0=db.Update time indices rcols newvals
      expect←data[rcoli]
      :For i :In ⍳⍴rcoli
          t←i⊃expect ⋄ t[ix]←i⊃newvals ⋄ (i⊃expect)←t
      :EndFor
      TEST←'Reading two columns for all ',(⍕numrecs),' records'
      assert expect≡db.Read time(1,⍪⊂⍳numrecs)rcols
     
      :If LOG
          ⎕←'Basic tests: memstats before db.Erase:'
          ⎕←memstats 0 ⍝ Report
      :EndIf
     
      TEST←'Deleting the db'
      assert 0={db.Erase}time ⍬
     
      z←'Creation Tests Completed'
    ∇
    
:EndNamespace