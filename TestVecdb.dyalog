﻿:Namespace TestVecdb

    ⍝ Updated to version 0.2.6 with mapped columns
    ⍝ Call TestVecdb.Run '' to run all tests
    ⍝   assumes vecdb is loaded in #.vecdb
    ⍝   returns memory usage statistics (result of "memstats 0")        

    (⎕IO ⎕ML)←1 1

    ∇ z←Run selection;path;source;tests;i;TIMELOG;LOG;m
     LOG←1
      ⎕FUNTIE ⎕FNUMS ⋄ ⎕NUNTIE ⎕NNUMS
      :Trap 6 ⋄ source←SALT_Data.SourceFile
      :Else ⋄ source←⎕WSID
      :EndTrap   
      path←{(-⌊/(⌽⍵)⍳'\/')↓⍵}source  

      ⎕←'Testing vecdb version ',#.vecdb.Version  
      :If selection≡'required' ⋄ selection←'' ⋄ :EndIf ⍝ Nothing like that yet  
      
      tests←{⍵/⍨(⊂'test_')∊⍨5↑¨⍵}⎕NL-3 
      :If 0≠≢selection
         :If 1=≡selection ⋄ selection←,⊂selection ⋄ :EndIf
         :If ∧/m←selection∊5↓¨tests ⋄ tests←'test_'∘,¨selection
         :Else ⋄ ('tests not found: ',(~m)/selection) ⎕SIGNAL 11
         :EndIf
      :EndIf           
      
      :For i :In ⍳≢tests
         TIMELOG←0 2⍴0
         ⍎i⊃tests
         :If LOG∧0≠≢TIMELOG
             ⎕←(i⊃tests) TIMELOG
         :EndIf
      :EndFor
    ∇
    
    ∇ (name folder)←preTest dummy
      name←⊃1↓⎕SI
      folder←'./',name,'/'
      ⎕←'Clearing: ',folder
      :Trap 22 ⋄ #.vecdb.Delete folder ⋄ :EndTrap
    ∇
    
    ∇ (db data columns types)←makeBasicDB numrecs;folder;name;range;types;tnms;recs;options;params;charvalues

      memstats 1       ⍝ Clear memory statistics
      (numrecs recs)←2↑numrecs,numrecs
      :If (100×numrecs)>2000⌶16                       
          ⎕←'*** Warning: workspace size should be at least: ',(⍕⌈(100×numrecs)÷1000000)',Mb ***'
      :EndIf
     
      folder←path,'/',(name←⊃1↓⎕SI),'/'
      ⍝⎕←'Clearing: ',folder
      :Trap 22 ⋄ #.vecdb.Delete folder ⋄ :EndTrap
     
      ⍝⎕←'Creating: ',folder←path,'/',name,'/'
      columns←'col_'∘,¨types←#.vecdb.TypeNames
      assert #.vecdb.TypeNames≡tnms←'I1' 'I2' 'I4',,¨'FBC' ⍝ Types have been added?
      range←2*¯1+8×1 2 4 6 0.25
      data←numrecs⍴¨¯1+⍳¨numrecs⌊range
      data←data×0.1*'F'=⊃¨(≢data)↑types ⍝ Make float values where necessary
      data←data,⊂numrecs⍴charvalues←{1↓¨(⍵=⊃⍵)⊂⍵}'/zero/one/two/three/four/five/six/seven/eight/nine/ten/eleven/one dozen/thirteen/fourteen/fifteen'
     
      :If LOG ⋄ ⎕←'Size of input data: ',fmtnum ⎕SIZE'data' ⋄ :EndIf
     
      (options←⎕NS'').BlockSize←numrecs(⌊×)0.6 ⍝ Provoke block overflow
      params←name folder columns types options(recs↑¨data)
      TEST←'Creating db & inserting ',(fmtnum recs),' records'
      db←⎕NEW time #.vecdb params
      assert db.isOpen
      assert db.Count=recs
      assert 0=db.Close
      assert 0=db.isOpen
     
      TEST←'Reopen database'
      db←(⎕NEW time)#.vecdb(,⊂folder) ⍝ Open it again
      assert db.isOpen
      assert db.Count=recs
     ∇

    :Section Tests

    ∇ test_sharding;columns;data;options;params;folder;types;name;db;ix;rotate;newcols;colsnow;m;db1;db2;ix2;ix1;t;i;z
     ⍝ Test database with 2 shards
     ⍝ Also acts as test for add/remove columns
     
      folder←path,'/',(name←'shardtest'),'/'
     
      :For rotate :In 0 1 2 ⍝ Test with shard key in all positions
     
          ⎕←'Clearing: ',folder
          :Trap 22 ⋄ #.vecdb.Delete folder ⋄ :EndTrap
     
          columns←rotate⌽'Name' 'BlockSize' 'Flag'
          types←rotate⌽,¨'C' 'F' 'C'
          data←rotate⌽('IBM' 'AAPL' 'MSFT' 'GOOG' 'DYALOG')(160.97 112.6 47.21 531.23 999.99)(5⍴'Buy' 'Sell')
     
          options←⎕NS''
          options.BlockSize←10000
          options.ShardFolders←(folder,'Shard')∘,¨'12'
          options.(ShardFn ShardCols)←'{2-2|⎕UCS ⊃¨⊃⍵}'(⊃rotate⌽1 3 2)
     
          params←name folder columns types options(3↑¨data)
          TEST←'Create sharded database (rotate=',(⍕rotate),')'
          db←⎕NEW time #.vecdb params
          assert 3=db.Count
          assert(3↑¨data)≡db.Read(1 2⍴1(1 2 3))columns ⍝ All went into shard #1
     
          TEST←'Append last 2 records'
          z←db.Append time columns(3↓¨data)
     
          assert 5=db.Count
          ix←db.Query('Name'((columns⍳⊂'Name')⊃data))⍬ ⍝ Should find everything
          assert(1 2,⍪⍳¨4 1)≡ix
          TEST←'Read it all back'
          assert data≡db.Read time ix columns
     
          newcols←columns,¨'2'
          TEST←'Add columns'
          z←db.AddColumns time newcols types
          z←db.Update ix newcols data ⍝ Populate new columns
          assert(db.Read ix columns)≡(db.Read ix newcols)
     
          TEST←'Remove columns'
          m←(⍳≢columns)≠db.ShardCols ⍝ not the shard col
          z←db.RemoveColumns time(m/columns),(~m)/newcols
          colsnow←((~m)/columns),m/newcols
          types←((~m)/types),m/types
          data←((~m)/data),m/data
          assert(db.(Columns Types))≡(colsnow types) ⍝ should now only have the new columns
          assert data≡db.Read ix colsnow        ⍝ Check database is "undamaged"
     
          z←db.Close
     
          ⍝ Now open shards individually
          db1←⎕NEW #.vecdb(folder 1)
          db2←⎕NEW #.vecdb(folder 2)
          ix1←db1.Query('Name'((colsnow⍳⊂'Name')⊃data))⍬ ⍝ Find all records
          ix2←db2.Query('Name'((colsnow⍳⊂'Name')⊃data))⍬ ⍝ ditto
          assert(1 2,⍪⍳¨4 1)≡ix1⍪ix2
          assert data≡⊃,¨/(db1 db2).Read(ix1 colsnow)(ix2 colsnow)
     
          t←4↓¨data
          'data may only be appended to opened shards'db1.Append expecterror colsnow t
          t[i]←⌽¨¨t[i←colsnow⍳⊂'Flag2']
          'new strings not allowed unless all shards are open'db2.Append expecterror colsnow t
          z←(db1 db2).Close
     
          TEST←'Erase database'
          db←⎕NEW #.vecdb(,⊂folder)
          assert 0={db.Erase}time ⍬
     
      :EndFor ⍝ rotate
     
      z←'Sharding Tests Completed'
    ∇

    ∇ z←test_basic;db;data;columns;numrecs;recs;TEST;select;where;expect;vals;indices;rcols;rcoli;types;ix;newvals;i;t
     ⍝ Create and delete some tables
     
      (db data columns types)←makeBasicDB (numrecs recs)←1 0.5×10000000 ⍝ 10 million records
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
      expect←⌽(⊃∧/data[1 6]∊¨(1 2 3)vals)∘/¨data                   ⍝ Reduced expectations
      TEST←'Two expression query'
      assert expect≡db.Query time where select
     
      TEST←'Single key, single data group by'
      expect←(1⊃data){⍺,+/⍵}⌸2⊃data
      assert expect≡db.Query time ⍬'sum col_I2' 'col_I1' ⍝ select sum(col_I2) group by col_I1'
     
      TEST←'Single CHAR key, single data group by'
      expect←(6⊃data){⍺,+/⍵}⌸2⊃data
      assert expect≡db.Query time ⍬'sum col_I2' 'col_C' ⍝ select sum(col_I2) group by col_C'
     
      TEST←'Single key, multiple data group by'
      expect←(1⊃data){⍺,(+/⍵[;1]),⌈/⍵[;2]}⌸↑[0.5]data[2 3]
      assert expect≡db.Query time ⍬('sum col_I2' 'max col_I4')'col_I1' ⍝ select sum(col_I2),max(col_I4) group by col_I1'
     
      TEST←'Two key, single data group by'
      expect←(↑[0.5]data[1 5]){⍺,+/⍵}⌸2⊃data
      assert expect≡db.Query time ⍬'sum col_I2'('col_I1' 'col_B') ⍝ select sum(col_I2) group by col_I1, col_B'
     
      TEST←'Two key, multiple data group by'
      expect←(↑[0.5]data[1 5]){⍺,(+/⍵[;1]),⌈/⍵[;2]}⌸↑[0.5]data[2 3]
      assert expect≡db.Query time ⍬('sum col_I2' 'max col_I4')('col_I1' 'col_B') ⍝ select sum(col_I2),max(col_I4) group by col_I1,col_B'     
     
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
     
      TEST←'Deleting the db' ⋄  assert 0={db.Erase}time ⍬
    ∇ 
    
    ∇test_calcmap;db;data;columns;numrecs;I1;Odd;OddC;charvalues;charsmapped;expect;allodd;square;sel;types;chardata
      ⍝ Test calculated / mapped columns  
      
      numrecs←10000
      (db data columns types)←makeBasicDB numrecs
      charvalues←∪chardata← (types⍳⊂,'C')⊃data

      (I1 Odd)←{⍵(2|⍵)}∪1⊃data              ⍝ Mappings of I1 column (with values in range 0…127)
      OddC←('Even' 'Odd')[1+Odd]            ⍝ Odd in Char form
      db.AddCalc'OddI1' 'col_I1' 'B' 'map'(I1 Odd) ⍝ name source type calculation data
      db.AddCalc'OddI1C' 'col_I1' 'C' 'map'(I1 OddC) ⍝ Map I1 => string 'Odd' or 'Even'
      db.AddCalc'SquareI1' 'col_I1' 'I2' '{⍵*2}'⍬'{⍵*0.5}' ⍝ Function with inverse for faster searches
      db.AddCalc'ThreeResC' 'col_C' 'C' 'map'(charvalues(charsmapped←16⍴'zero' 'one' 'two')) ⍝ Map on char=>char
     
      assert Odd≡db.Calc'OddI1'I1           ⍝ Check that we perform a calculation
      assert OddC≡db.Calc'OddI1C'I1
      assert charsmapped≡db.Calc'ThreeResC'charvalues
     
      TEST←'Select calculated column'
      expect←({↓⍉(⍵∘.*1 2),2|⍵}1⊃data),(('Even' 'Odd')[1+2|1⊃data])(('zero' 'one' 'two')[1+3|¯1+charvalues⍳chardata])
      assert expect≡db.Query time ⍬('col_I1' 'SquareI1' 'OddI1' 'OddI1C' 'ThreeResC') ⍝ select col_I1, SquareI1, OddI1 ThreeResC
     
      TEST←'Test query on calculated column with inverse'
      expect←1 2 3
      assert expect≡∪⊃db.Query('SquareI1'(1 4 9))'col_I1' ⍝ select col_I1 where SquareI1 in 1 4 9
     
      expect←((≢charvalues)⍴0 1 0)/charvalues
      assert expect≡∪⊃db.Query('ThreeResC'(⊂'one'))'col_C' ⍝ Where clause on char=>char mapping
     
      TEST←'Group by calculation'
      expect←(allodd←2|1⊃data){⍺,+/⍵}⌸2⊃data
      assert expect≡db.Query time ⍬'sum col_I2' 'OddI1'      ⍝ select sum(col_i2) group by OddI1
      TEST←'Group by 1 calc, filter on another'
      sel←(square←×⍨1⊃data)∊1 4 9 ⍝ where (I2*2)∊1 4 9
      expect←(sel/square){⍺,+/⍵}⌸sel/2⊃data
      assert expect≡db.Query time('SquareI1'(1 4 9))'sum col_I2' 'SquareI1' ⍝ select sum(col_i2) group by SquareI1 where SquareI1∊1 4 9
     
      db.RemoveCalc'OddI1'
      ⍝ /// More calc column QA required
      ⍝ /// Do not allow calcs on character columns   

      TEST←'Deleting the db' ⋄  assert 0={db.Erase}time ⍬
    ∇

    ∇ test_add_columns_in_sequence;name;folder;options;numrecs;columns;types;params;db;data
      name folder←preTest ⍬
     
      numrecs←10
      columns←'first' 'second' 'third'
      types←'I1' 'F' 'C'
      data←(numrecs(?⍴)127)(0.1×numrecs(?⍴)1000)(numrecs⍴'abc' 'def' 'xyz')
      (options←⎕NS'').BlockSize←8 ⍝ Provoke block overflow
      params←name folder(,1⌷columns)(,1⌷types)options(,1⌷data)
      TEST←'Creating db & inserting columns in sequence'
      db←⎕NEW time #.vecdb params
      assert(,1⌷data)≡db.Read(1,⍪⎕NULL)(1⊃columns)
      db.AddColumns,¨2⌷¨columns types
      db.Update(1,⍪⍳10)(2⊃columns)(2⊃data)
      assert(,2⌷data)≡db.Read(1,⍪⎕NULL)(2⊃columns)
      db.AddColumns 3⌷¨columns types
      db.Update(⊂1,⍪⍳10),3⊃¨columns data
      assert(3⊃data)≡⊃db.Read(1,⍪⎕NULL)(3⊃columns)
      assert 0=db.Erase
    ∇

    ∇ test_define_block_size;name;folder;options;params;db;em
      name folder←preTest ⍬
      (options←⎕NS'').BlockSize←10
      params←name folder(,⊂'field')(,⊂'I1')options
      em←'Block size must be a multiple of 8'
      em ⎕NEW expecterror #.vecdb params
      options.BlockSize←64
      db←⎕NEW #.vecdb params
      assert db.isOpen
      assert 0=db.Erase
    ∇

    ∇ no_test_summary_fns;name;folder;options;columns;types;params;db;data;sort;comp
      name folder←preTest ⍬
     
      columns←'id' 'name' 'price' 'quantity'
      types←'I1' 'C' 'F' 'I1'
      data←,⊂9⍴1 2
      data,←⊂3/'ett' 'due' 'three'
      data,←⊂0.25×⍳9
      data,←⊂⌽⍳9
     
      options←⎕NS''
      options.ShardFolders←(folder,'Shard')∘,¨'12'
      options.(ShardFn ShardCols)←'{2-2|⊃⍵}' 1
     
      params←name folder columns types options data
      db←⎕NEW #.vecdb params
      sort←{(⊂⍋↑⊃↓⍉⍵)⌷⍵}
      comp←sort⍨≡sort
      assert(+⌿⍉↑data[3 4])comp db.Query ⍬('sum price' 'sum quantity')⍬
      assert((1⊃data){⍺,+⌿⍵}⌸⍉↑data[3 4])comp db.Query ⍬('sum price' 'sum quantity')'id'
      assert((2⊃data){⍺,+⌿⍵}⌸⍉↑data[3 4])comp db.Query ⍬('sum price' 'sum quantity')'name'
      assert((⍉↑2↑data){⍺,+⌿⍵}⌸⍉↑data[3 4])comp db.Query ⍬('sum price' 'sum quantity')('id' 'name')
     
      assert(⌈⌿⍉↑data[3 4])comp db.Query ⍬('max price' 'max quantity')⍬
      assert((1⊃data){⍺,⌈⌿⍵}⌸⍉↑data[3 4])comp db.Query ⍬('max price' 'max quantity')'id'
      assert((2⊃data){⍺,⌈⌿⍵}⌸⍉↑data[3 4])comp db.Query ⍬('max price' 'max quantity')'name'
      assert((⍉↑2↑data){⍺,⌈⌿⍵}⌸⍉↑data[3 4])comp db.Query ⍬('max price' 'max quantity')('id' 'name')
     
      assert(⌊⌿⍉↑data[3 4])comp db.Query ⍬('min price' 'min quantity')⍬
      assert((1⊃data){⍺,⌊⌿⍵}⌸⍉↑data[3 4])comp db.Query ⍬('min price' 'min quantity')'id'
      assert((2⊃data){⍺,⌊⌿⍵}⌸⍉↑data[3 4])comp db.Query ⍬('min price' 'min quantity')'name'
      assert((⍉↑2↑data){⍺,⌊⌿⍵}⌸⍉↑data[3 4])comp db.Query ⍬('min price' 'min quantity')('id' 'name')
     
      assert(2/≢⊃data)comp db.Query ⍬('count price' 'count quantity')⍬
      assert((1⊃data){⍺,2/≢⍵}⌸⍉↑data[3 4])comp db.Query ⍬('count price' 'count quantity')'id'
      assert((2⊃data){⍺,2/≢⍵}⌸⍉↑data[3 4])comp db.Query ⍬('count price' 'count quantity')'name'
      assert((⍉↑2↑data){⍺,2/≢⍵}⌸⍉↑data[3 4])comp db.Query ⍬('count price' 'count quantity')('id' 'name')
     
      assert 0=db.Erase
    ∇

    :EndSection

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
          z←⍺ ⍺⍺ ⍵
          z⊣timelog TEST (⎕AI[3]-t)
      }                   
      
      ∇{info}←timelog info
       :If 2=⎕NC 'TIMELOG'
          TIMELOG⍪←info
       :EndIf
      ∇

      expecterror←{
          0::⎕SIGNAL(⍺≡⊃⎕DMX.DM)↓11
          z←⍺⍺ ⍵
          ⎕SIGNAL 11
      }

:EndNamespace
