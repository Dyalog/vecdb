:Namespace TestVecdbSrv
    ⍝ Call TestVecdbSrv.RunAll to run Server Tests
    ⍝   assumes existence of #.vecdbclt and #.vecdb

    (⎕IO ⎕ML)←1 1
    LOG←1
    toJson←(0 1)∘(7160⌶)

    ∇ z←Benchmark;columns;data;options;params;folder;types;name;ix;users;srvproc;clt;TEST;config;db;path;tn;folders;run;sym;date;price;lix;expect;⎕RL;m
     ⍝ Test database with 2 shards
     ⍝ Also acts as test for add/remove columns
     
      ⍝ --- Create configuration file ---
     
      :For run :In 1↓'serial' 'parallel' ⍝ /// While debugging parallel
          ⎕←'*** ',run,' run begins...'
     
          columns←'Day' 'Sym' 'Price'
          types←,¨'I2' 'C' 'F'
          data←⍬ ⍬ ⍬
     
          :Select run
          :Case 'serial' ⋄ path←Init
          :Case 'parallel' ⋄ path←Init ⍝ '//Mortens-Macbook-Air/vecdb'
          :EndSelect
     
          folder←path,'/',(name←'srvtest'),'/'
     
          ⎕←'Clearing: ',folder
          :Trap 22 ⋄ {}#.vecdb.Delete folder ⋄ :EndTrap
          1 ⎕MKDIR folder
     
          config←CreateBenchConfig folder,'config.json'
     
          options←⎕NS''
          options.BlockSize←10000 ⍝ 10,000 records

          date←10000/⍳2×365       ⍝ 10,000 records × 2 × 365 days = ~36M records
          sym←(≢date)⍴'MSFT' 'IBM' 'AAPL' 'GOOG' 'DYLG'
          price←100+0.1×⍳⍴date  ⍝ It's a good market!
          
          :Select run
          :Case 'serial'     
              options.ShardFolders←⍬
              params←name folder columns types options (5↑¨date sym price)
              TEST←'Create empty database'
              db←{⎕NEW #.vecdb params} time ⍬
              assert 5=db.Count
     
          :Case 'parallel'           
           ⍝   options.ShardFolders←'c:\devt\vecdb\srvtest\shard1' '//Mortens-Macbook-Air/vecdb/srvtest/shard2'
           ⍝   options.LocalFolders←'c:\devt\vecdb\srvtest\shard1' '//Users/mkrom/vecdb/srvtest/shard2'
              options.ShardFolders←'c:\devt\vecdb\srvtest\shard1' 'c:\devt\vecdb\srvtest\shard2'
              options.LocalFolders←'c:\devt\vecdb\srvtest\shard1' 'c:\devt\vecdb\srvtest\shard2'
           
              options.(ShardFn ShardCols)←'{1+2|⊃⍵}' 1 ⍝ Odd/Even day number
              params←name folder columns types options (5↑¨date sym price) ⍝ pre-populate symbols
              TEST←'Create empty database'
              db←{⎕NEW #.vecdb params} time ⍬
              assert 5=db.Count
              db.Close

      ⍝ ↓↓↓ rewrite this only require "folder" and read the rest from JSON config       
      ∘∘∘
              srvproc←#.vecdbsrv.Launch folder 8100 ⍬'c:\devt\vecdb\vecdbboot.dws'
              #.vecdbclt.Connect'127.0.0.1' 8100 'mkrom'
              db←#.vecdbclt.Open folder
          :EndSelect
     
          TEST←'Append Data'               
          z←db.Append time columns(5↓¨date sym price)
     
          TEST←'Count records'
          assert(≢date)={db.Count}time ⍬
     
          TEST←'Search for two days of records'
          ix←db.Query time('Day'(200 201))⍬ ⍝ Should find 2,000 records
          assert (+/date∊200 201)=≢∊ix[;2]
     
          TEST←'Read 2 days records (2,000)'
          lix←{⍵/⍳⍴⍵}date∊200 201
          expect←{⍵[lix]}¨date sym price
          assert expect≡db.Query time ('Day'(200 201)) columns  
          
          TEST←'count Price, max Price, min Price group by Day (1st run)'
          m←1
          expect←(m/date){(⊂⍺),(≢⍵),(⌈/⍵),⌊/⍵}⌸m/price
          assert expect≡{⍵[⍋⍵[;1];]}db.Query time ⍬ ('count Price' 'max Price' 'min Price')'Day' 

          TEST←'count Price, max Price, min Price group by Day (2nd run)'
          assert expect≡{⍵[⍋⍵[;1];]}db.Query time ⍬ ('count Price' 'max Price' 'min Price')'Day' 
     
          :If run≡'parallel'
              ⎕←'Closing down server...'
              z←db.Shutdown'Shutting down now!'
              ⎕DL 3
              :If ~srvproc.HasExited ⋄ z←srvproc.Kill ⋄ :EndIf
              ⎕DL 3
          :EndIf
     
          TEST←'Erase database'
          db←⎕NEW #.vecdb(,⊂folder)
          assert 0={db.Erase}time ⍬ 
          1 ⎕NDELETE folder
     
      :EndFor
     
      z←'Server Tests Completed'
    ∇

    ∇ path←Init;source
      ⎕FUNTIE ⎕FNUMS ⋄ ⎕NUNTIE ⎕NNUMS
      :Trap 6 ⋄ source←SALT_Data.SourceFile
      :Else ⋄ source←⎕WSID
      :EndTrap
      path←{(-⌊/(⌽⍵)⍳'\/')↓⍵}source
      :If 0=⎕NC'#.DRC' ⋄ 'DRC'#.⎕CY'conga' ⋄ :EndIf
    ∇

    ∇ z←RunAll;path;source
      ⎕←ServerBasic
    ∇

    ∇ config←CreateBenchConfig filename;db;config;user;vecdbsrv;cmd;host;keyfile;userid;port
     ⍝
      cmd←'RIDE_SPAWNED=1 RIDE_INIT=SERVE::5678 /Applications/Dyalog-15.0.app/Contents/Resources/Dyalog/mapl'
      host←'Mortens-Macbook-Air'
      userid←'mkrom'
      keyfile←'c:\docs\personal\macbook-air'
      port←8100
     
      user←⎕NS''
      user.(Name Id Admin)←'mkrom' 1001 1
      vecdbsrv←⎕NS''
      vecdbsrv.Name←'Test Server'
      vecdbsrv.BootWs←'c:\devt\vecdb\bootvecdb.dws'
      vecdbsrv.Port←port
      vecdbsrv.Users←,user
      db←⎕NS''
      db.Folder←folder
      db.Slaves←⎕NS¨2⍴⊂''
      db.Slaves.Shards←,¨1 2 ⍝ Distribution of shards to slave processors
      db.Slaves.Folder←⊂folder ⍝ Root folder
      db.Slaves[1].(Launch←⎕NS'').Type←'local'
      db.Slaves[2].(Launch←⎕NS'').Type←'local'
      db.Slaves.Folder←⊂folder ⍝ Local test only right now
    ⍝  db.Slaves[1].Folder←'//Mortens-Macbook-Air/vecdb/srvtest/' ⍝ If different seen from this slave
    ⍝  db.Slaves[2].(Launch←⎕NS'').(Type Host User KeyFile Cmd)←'ssh'host userid keyfile cmd
    ⍝  db.Slaves[2].Folder←'/Users/mkrom/vecdb/srvtest/'
    
      config←⎕NS''
      config.Server←vecdbsrv
      config.DBs←,db
      (toJson config)⎕NPUT filename
    ∇

    ∇ config←CreateTestConfig filename;db;config;user;vecdbsrv
     ⍝
      user←⎕NS''
      user.(Name Id Admin)←'mkrom' 1001 1
      vecdbsrv←⎕NS''
      vecdbsrv.Name←'Test Server'
      vecdbsrv.Users←,user
      db←⎕NS''
      db.Folder←folder
      db.Slaves←⎕NS¨2⍴⊂''
      db.Slaves.Shards←,¨1 2 ⍝ Distribution of shards to slave processors
      db.Slaves.(Launch←⎕NS'').Type←⊂'local'
      config←⎕NS''
      config.Server←vecdbsrv
      config.DBs←,db
      (toJson config)⎕NPUT filename
    ∇

    ∇ (db params)←CreateTestDB;columns;types;data;options
      columns←'Name' 'Price' 'Flag'
      types←,¨'C' 'F' 'C'
      data←('IBM' 'AAPL' 'MSFT' 'GOOG' 'DYALOG')(160.97 112.6 47.21 531.23 999.99)(5⍴'Buy' 'Sell')
     
      options←⎕NS''
      options.ShardFolders←(folder,'Shard')∘,¨'12'
      options.BlockSize←10000
      options.(ShardFn ShardCols)←'{2-2|⎕UCS ⊃¨⊃⍵}' 1
     
      params←name folder columns types options data
      db←⎕NEW #.vecdb params
      assert(≢⊃data)=db.Count
    ∇

    ∇ z←ServerBasic;columns;data;options;params;folder;types;name;ix;users;srvproc;clt;TEST;config;db;path
     ⍝ Test database with 2 shards
     ⍝ Also acts as test for add/remove columns
     
      path←Init
      folder←path,'/',(name←'srvtest'),'/'
      ⎕←'Clearing: ',folder
      :Trap 22 ⋄ {}#.vecdb.Delete folder ⋄ :EndTrap
      ⎕MKDIR folder
     
      ⍝ --- Create configuration file ---
     
      config←CreateBenchConfig folder,'config.json'
      (db(name folder columns types options data))←CreateTestDB
     
      ⍝ --- Launch and connect to server, open database ---
     
      srvproc←#.vecdbsrv.Launch folder 8100
      #.vecdbclt.Connect'127.0.0.1' 8100 'mkrom'
      db←#.vecdbclt.Open folder
     
      TEST←'Count records'
      assert(≢⊃data)={db.Count}time ⍬
     
      TEST←'Search for all records'
      ix←db.Query time('Name'((columns⍳⊂'Name')⊃data))⍬ ⍝ Should find everything
      assert(1 2,⍪⍳¨4 1)≡ix
     
      TEST←'Read it all back'
      assert data≡db.Read time ix columns
     
      (2⊃data)×←1.1 ⍝ Add 10% to all prices
      TEST←'Update prices'
      z←db.Update time ix'Price'(2⊃data) ⍝ Update price
      assert data≡db.Read ix columns
     
      ⍝ /// Tests to do:
      ⍝ Append data - to all and less that all shards
      ⍝ Update multiple columns
      ⍝ Read & update records with shards "out of order"
      ⍝ Read & update not from all shards
     
      ⎕←'Closing down server...'
      z←db.Shutdown'Shutting down now!'
      ⎕DL 3
      :If ~srvproc.HasExited ⋄ srvproc.Kill ⋄ :EndIf
      ⎕DL 3
     
      TEST←'Erase database'
      db←⎕NEW #.vecdb(,⊂folder)
      assert 0={db.Erase}time ⍬
     
      z←'Server Tests Completed'
    ∇

    ∇ x←output x
      :If LOG ⋄ ⍞←x ⋄ :EndIf
    ∇

    ∇ r←fmtnum x
    ⍝ Nice formatting of large integers
      r←(↓((⍴x),20)⍴'CI20'⎕FMT⍪,x)~¨' '
    ∇

    assert←{'Assertion failed'⎕SIGNAL(⍵=0)/11}

      time←{⍺←⊣ ⋄ t←⎕AI[3]
          o←output TEST,' ... '
          z←⍺ ⍺⍺ ⍵
          o←output(⍕⎕AI[3]-t),'ms',⎕UCS 10
          z
      }

      expecterror←{
          0::⎕SIGNAL(⍺≡⊃⎕DMX.DM)↓11
          z←⍺⍺ ⍵
          ⎕SIGNAL 11
      }

:EndNamespace
