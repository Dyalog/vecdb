:Namespace TestVecdb

    ⍝ Call TestVecdb.RunAll to fun a full system test
	⍝   assumes vecdb is loaded in #.vecdb
	⍝   returns memory usage statistics (result of "memstats 0")
	
    (⎕IO ⎕ML)←1 1    
    LOG←1  	
	
	∇ z←RunAll;path
      ⎕FUNTIE ⎕FNUMS ⋄ ⎕NUNTIE ⎕NNUMS
      path←{(-⌊/(⌽⍵)⍳'\/')↓⍵}⎕WSID
      ⎕←Basic
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
    
    ∇ z←Basic;columns;types;folder;name;db;tnms;data;numrecs;recs;select;where;expect;indices;options;params
     ⍝ Create and delete some tables
     
      memstats 1 ⍝ Clear memory statistics
     
      ⎕←'Creating: ',folder←path,'\',(name←'testdb1'),'\'
      :Trap 11 ⋄ {}(⎕NEW #.vecdb(,⊂folder)).Erase ⋄ :EndTrap
     
      columns←'col_'∘,¨types←#.vecdb.TypeNames
      assert #.vecdb.TypeNames≡tnms←'I1' 'I2' 'I4'(,'F') ⍝ Types have been added?
     
      numrecs←5000000 ⍝ 5 million records
      data←numrecs⍴¨⍳¨numrecs⌊¯1+2*¯1+8×(1 2 4 6)[tnms⍳types]
      data←data×0.1*'F'∊¨types ⍝ Make float values where necessary
     
      :If LOG
          ⎕←'Size of input data: ',fmtnum ⎕SIZE'data'
      :EndIf
     
      recs←numrecs(⌊÷)2
      (options←⎕NS'').BlockSize←numrecs(⌊×)0.6 ⍝ Provoke block overflow
      params←name folder columns types options(recs↑¨data)
      TEST←'Creating db & inserting ',(fmtnum recs),' records'
      db←⎕NEW time #.vecdb params
      assert db.Open
      assert options.BlockSize∧.=db.(BlockSize,Size)
      assert 0=db.Close
      assert 0=db.Open
     
      TEST←'Reopen database'
      db←⎕NEW time #.vecdb(,⊂folder) ⍝ Open it again
      assert db.Open
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
     
      where←where((2⊃columns)(1 2 3))         ⍝ Add 2nd filter
      expect←⌽(⊃∧/(2↑data)∊¨⊂1 2 3)∘/¨data    ⍝ Reduced expectations
      TEST←'Two expression query'
      assert expect≡db.Query time where select
     
      ⍝ Test vecdb.Replace
      indices←db.Query where ⍬
      TEST←'Updating ',(fmtnum≢indices),' records'
      assert 0=db.Update time indices(2⊃columns)(-(2⊃data)[indices]) ⍝ Update with 0-data
      expect←2⊃data ⋄ expect[indices]←-expect[indices]
      TEST←'Reading one column for all ',(⍕numrecs),' records'
      assert expect≡⊃db.Read time(⍳numrecs)(2⊃columns)
     
      :If LOG
          ⎕←'Basic tests: memstats before db.Erase:'
          ⎕←memstats 0 ⍝ Report
      :EndIf
     
      TEST←'Deleting the db'
      assert 0={db.Erase}time ⍬
     
      z←'Creation Tests Completed'
    ∇
    
:EndNamespace