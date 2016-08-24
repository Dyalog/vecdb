:Namespace TestVecdbSrv
    ⍝ Call TestVecdbSrv.RunAll to run Server Tests
    ⍝   assumes existence of #.vecdbclt and #.vecdb

    (⎕IO ⎕ML)←1 1
    LOG←1
    toJson←(0 1)∘(7160⌶)

    ∇ z←RunAll;path;source
      ⎕FUNTIE ⎕FNUMS ⋄ ⎕NUNTIE ⎕NNUMS
      :Trap 6 ⋄ source←SALT_Data.SourceFile
      :Else ⋄ source←⎕WSID
      :EndTrap
      path←{(-⌊/(⌽⍵)⍳'\/')↓⍵}source
      ⎕←ServerBasic
    ∇
   
    ∇ config←CreateTestConfig filename;db;config;user;vecdbsrv
     ⍝ 
      user←⎕NS ''
      user.(Name Id Admin)←'mkrom' 1001 1
      vecdbsrv←⎕NS''
      vecdbsrv.Name←'Test Server'
      vecdbsrv.Users←,user
      db←⎕NS''
      db.Folder←folder
      db.Slaves←,¨1 2 ⍝ Distribution of shards to slave processors
      config←⎕NS''
      config.Server←vecdbsrv
      config.DBs←,db
      (toJson config)⎕NPUT filename
    ∇

    ∇ z←ServerBasic;columns;data;options;params;folder;types;name;ix;users;srvproc;clt;TEST
     ⍝ Test database with 2 shards
     ⍝ Also acts as test for add/remove columns
     
      folder←path,'/',(name←'srvtest'),'/'
      ⎕←'Clearing: ',folder
      :Trap 22 ⋄ #.vecdb.Delete folder ⋄ :EndTrap
      ⎕MKDIR folder
     
      ⍝ --- Create configuration file ---

      CreateTestConfig folder,'config.json'
            
      ⍝ --- Create database ---

      columns←'Name' 'BlockSize' 'Flag'
      types←,¨'C' 'F' 'C'
      data←('IBM' 'AAPL' 'MSFT' 'GOOG' 'DYALOG')(160.97 112.6 47.21 531.23 999.99)(5⍴'Buy' 'Sell')
     
      options←⎕NS''
      options.BlockSize←10000
      options.ShardFolders←(folder,'Shard')∘,¨'12'
      options.(ShardFn ShardCols)←'{2-2|⎕UCS ⊃¨⊃⍵}' 1
     
      params←name folder columns types options data
      TEST←'Create sharded database'
      db←⎕NEW #.vecdb params
      assert (≢data)=db.Count

      ⍝ --- Launch and connect to server, open database ---

      srvproc←#.vecdbsrv.Launch folder 8100
      assert 0=srvproc.HasExited
      
      clt←#.vecdbclt.Connect '127.0.0.1' 8100 'mkrom'
      db←clt.Open folder

      ix←db.Query('Name'((columns⍳⊂'Name')⊃data))⍬ ⍝ Should find everything
      assert(1 2,⍪⍳¨4 1)≡ix
      TEST←'Read it all back'
      assert data≡db.Read time ix columns
          
      z←db.Close
      clt.ShutDown 'Shutting down now!'
      ⎕DL 3
      svrproc.Kill
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
