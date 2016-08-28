:Namespace vecdbsrv
    ⍝ Uses #.vecdbclt

    (⎕IO ⎕ML)←1 1
    RUNTIME←0 ⍝ Use runtimes? 
    NEXTPORT←8000            
    fromJSON←7159⌶

    ∇ {r}←Start (folder port);sink;data;event;obj;rc;wait;z;cmd;name
     ⍝ Run a vecdb Server - based on CONGA RPCServer sample
     
      NEXTPORT←port+1
      {}##.DRC.Init''
      CONFIG←fromJSON ⊃⎕NGET folder,'config.json'
      Init CONFIG
      {}##.DRC.Close name←'VECSRV' 
     
      :If 0=1⊃r←##.DRC.Srv name''port'Command'
          1 Log'Server ''',name,''', listening on port ',⍕port
          2 Log'Handler thread started: ',⍕Run&name port
      :Else
          3 Log'Server failed to start: ',,⍕r
      :EndIf
    ∇
    
    ∇ {r}←Shutdown msg;db;i;j;slave
     ⍝ Shutdown
     ⍝ /// Should validate user authorisation
     ⍝ /// Should broadcast msg to all users
      
      :For i :In ⍳≢DBs ⍝ Close all slaves
           db←i⊃DBs
           :For j :In ⍳≢db.Slaves
                slave←j⊃db.Slaves
                #.vecdbclt.SrvDo slave.Connection ('Shutdown' TOKEN)
                {}#.DRC.Close slave.Connection
           :EndFor
      :EndFor                      
      
      done←1          ⍝ Global flag to shut down
      r←⍬             ⍝ Need a result
    ∇

    ∇ {r}←Init config;db;i;j;slave
     ⍝ Intialise the vecdb server
      
      CONNS←TASKS←USERS←TOKENS←⍬
      NEXTTASK←1000
      LOGLEVEL←0

      (DBs Server)←config.(DBs Server)  
      TOKEN←{⎕RL←0 ⋄ ⎕PP←10 ⋄ 2↓⍕?0}0
      DBFolders←DBs.Folder

      :For i :In ⍳≢DBs ⍝ Launch all the processes
           db←i⊃DBs
           :For j :In ⍳≢db.Slaves
                slave←j⊃db.Slaves                      
                slave.Port←NEXTPORT
                slave.Address←'127.0.0.1' ⍝ /// for now
                slave.UserId←¯1           ⍝ /// ditto
                slave.Proc←slave.Shards Launch db.Folder slave.Port
                NEXTPORT←NEXTPORT+1
           :EndFor
      :EndFor                      
      
      :For i :In ⍳≢DBs ⍝ Now try to connect to them all
      ⍝ /// in future perhaps launch a thread for each one and just check status?
           db←i⊃DBs
           :For j :In ⍳≢db.Slaves
                slave←j⊃db.Slaves
                :If 0=⊃r←'' #.vecdbclt.Connect slave.(Address Port UserId)
                  slave.Connection←2⊃r  
                  #.vecdbclt.SrvDo slave.Connection ('SetToken' TOKEN)
                :Else
                   ∘∘∘ ⍝ start up failed
                :EndIf
           :EndFor
      :EndFor                      
      
      r←⍬             ⍝ Need a result
    ∇

    ∇ Process(obj data);r;CONNECTION;cmd;arg;close;txt;db;i;slave;rs
     ⍝ Process a call. data[1] contains function name, data[2] an argument
     
     ⍝ {}##.DRC.Progress obj('    Thread ',(⍕⎕TID),' started to run: ',,⍕data) ⍝ Send progress report
      CONNECTION←obj
      Conn←1↓⊃(obj='.')⊂obj
      (cmd arg)←2↑data
      close←0

      :If (⊂cmd)∊'Open' 'SetUser' 'Shutdown' ⍝ Non-DB commands
          :Trap 9999
               r←0 (⍎cmd,' obj arg')
          :Else ⋄ r←⎕EN ⎕DM
          :EndTrap     
     
      :ElseIf (⊂cmd)∊'Append' 'Count' 'Query' 'Update' 'Read' 
          :If (≢DBs)<i←DBFolders⍳arg[1]
              r←999 ('Database not found: ',⊃arg)
          :Else
             db←i⊃DBs
             rs←⍬
             :For slave :In db.Slaves 
                 rs,←⊂#.vecdbclt.SrvDo slave.Connection (cmd (2⊃arg))
             :EndFor   
             r←0 rs
          :EndIf
      :Else
          r←999 ('Unsupported command: ',cmd)
      :EndIf
     
      {}##.DRC.Respond obj r

      :If close
          ⍝ /// {{}##.DRC.Close ⍵⊣⎕DL 1}&Conn ⍝ Start thread which waits 1s then closes
      :EndIf     
    ∇
    
    ∇ proc←{shards} Launch (target port);path;runtime;args;slave;ws;source
     ⍝ Launch a full vecdbsrv or, if shards is defined, a slave
      
      :Trap 6 ⋄ source←SALT_Data.SourceFile
      :Else ⋄ source←⎕WSID
      :EndTrap 

      path←{(-⌊/(⌽⍵)⍳'\/')↓⍵}source  
      ws←path,'/vecdbboot.dws'
      runtime←RUNTIME
      
      :If slave←2=⎕NC 'shards'
          args←'VECDBSLAVE="',target,'" SHARDS="',(⍕shards),'" PORT=',(⍕port),' TOKEN="',TOKEN,'"'  
      :Else
          args←'VECDBSRV="',target,'" PORT=',(⍕port)
      :EndIf
      proc←⎕NEW ##.APLProcess (ws args runtime) 
    ∇

    ∇ Connect cmd;task;conn
     ⍝ Connection Arrived
     
      conn←1↓⊃(cmd='.')⊂cmd
     
      CONNS,←⊂conn
      TASKS,←task←NEXTTASK
      NEXTTASK←10000|NEXTTASK+1
      USERS←USERS,0
     
      0 Log'New connection ',conn,' assigned task id ',⍕task
    ∇

    ∇ Disconnect obj;m;i;held;task;conn
     ⍝ Connection Lost
     
      conn←1↓⊃(obj='.')⊂obj
      0 Log'Connection ',conn,' disconnected'
     
      :If (⍴m)≥i←(m←~CONNS∊⊂conn)⍳0
          task←i⊃TASKS
          :If 0≠⍴held←(HELDBY=task)/RESOURCES
              Release¨↓(⊂obj),[1.5]held ⍝ Release all held resources
          :EndIf
     
          QUEUES←{(⍵[;1]∊task)⌿⍵}¨QUEUES ⍝ Remove task from queues
          CONNS←m/CONNS
          TASKS←m/TASKS
          USERS←m/USERS
      :EndIf
    ∇

    ∇ level Log message
     
      →(level<LOGLEVEL)⍴0
      ⎕←(,'ZI2,<:>,ZI2,<:>,ZI2,<.>,ZI3'⎕FMT 1 4⍴3↓⎕TS),' ',message
    ∇

    ∇ MockTest;assert;START;resources;nprocesses;nresources;nevents;i;conns;conn;z;s
     
      assert←{'Assertion failed'⎕SIGNAL(⍵=0)/11}
     
      InitLocks 0
      LOGLEVEL←3 ⍝ Log everything
      MOCK←1
     
      Connect'C1'
      assert(1 0)≡TASKS,USERS
      SetUser'C1' 1234
      assert(1 1234)≡TASKS,USERS
     
      Connect'C2'
      SetUser'C2' 4321
     
      assert 0=Lock'C1' '/ALLOC10' ⍝ Granted
      assert HELDBY≡,1             ⍝ Held by Task 1
      Release'C1' '/ALLOC10'       ⍝ Release
      assert HELDBY≡,0             ⍝ Should now be free
     
      assert 0=Lock'C1' '/ALLOC10' ⍝ Granted
      assert HELDBY≡,1             ⍝ Held by Task 1
      assert 1=Lock'C2' '/ALLOC10' ⍝ Queued
      assert(2 'C2')≡2⍴⊃QUEUES     ⍝ Task 2 is in the queue
     
      Release'C1' '/ALLOC10'
      assert HELDBY≡,2             ⍝ Should now be held by Task 2
      assert 0=⊃⍴⊃QUEUES
     
      Disconnect'C2'
      assert 1=⍴TASKS
      assert HELDBY≡,0             ⍝ Should now be free
      Disconnect'C1'
      assert 0=⍴TASKS
     
     ⍝ --- performance test ---
     
      LOGLEVEL←3 ⍝ Erors only
     
      nprocesses←10
      nevents←1000×2×nprocesses
      ⎕←'Testing performance...'
      Connect¨conns←'C'∘,¨⍕¨⍳nprocesses
      SetUser¨↓conns,[1.5]⍳nprocesses
      resources←nevents⍴('/BLAH/BLAH/ALLOC'∘,¨⍕¨⍳nprocesses),nprocesses⍴⊂'/BLAH/BLAH/ALLOC0'
     
      START←3⊃⎕AI
      :For i :In ⍳nprocesses+nevents
          conn←(1+nprocesses|i-1)⊃conns
          :If i≤nevents ⋄ z←Lock conn(i⊃resources) ⋄ :EndIf
          :If i>nprocesses ⋄ z←Release conn((i-nprocesses)⊃resources) ⋄ :EndIf
      :EndFor
     
      s←0.001×(3⊃⎕AI)-START
      ⎕←(⍕nevents),' released & locked in',(1⍕s),'s (',(,' '~⍨,'CI12'⎕FMT nevents÷s),' locks/s)'
    ∇

    ∇ Notify(cmd Resource info);Conn;task
     ⍝ Notify connection that resource has been granted
     
      LOCKSGRANTED+←1
      :If LOGLEVEL=0
          Conn←1↓⊃(cmd='.')⊂cmd
          task←(CONNS⍳⊂Conn)⊃TASKS
          0 Log'Lock for ',Resource,' granted to task ',task
      :EndIf
     
      :If ~MOCK
          :If 0≠⊃r←#.DRC.Respond cmd(0(Resource info))
              1 Log'Respond to ',cmd,' failed'
          :EndIf
      :EndIf
    ∇

    ∇ r←Run(name port);sink;data;event;obj;rc;wait;z;cmd
     ⍝ Run the Lock Server - based on CONGA RPCServer sample
     
      :If 0=⎕NC'start' ⋄ start←1 ⋄ :EndIf
      {}##.DRC.Init''
     
      0 Log'Thread ',(⍕⎕TID),' is now handing server ''',name,'''.'
      done←0 ⍝ done←1 in function "End"
      :While ~done
          rc obj event data←4↑wait←##.DRC.Wait name 3000 ⍝ Time out now and again
     
          :Select rc
          :Case 0
              :Select event
              :Case 'Error'
                  :If 1119≢data ⋄ 3 Log'Error ',(⍕data),' on ',obj ⋄ :EndIf
                  :If ~done∨←name≡obj ⍝ Error on the listener itself?
                      {}##.DRC.Close obj ⍝ Close connection in error
                      Disconnect obj ⍝ Let logic know
                  :EndIf
     
              :Case 'Receive'
                  :If 2≠⍴data ⍝ Command is expected to be (function name)(argument)
                      {}##.DRC.Respond obj(99999 'Bad command format') ⋄ :Leave
                  :EndIf
          
                  Process obj data ⍝ NB Single-threaded
     
              :Case 'Connect' ⍝ Ignored
                  Connect obj
     
              :Else ⍝ Unexpected result?
                  ∘
              :EndSelect
     
          :Case 100  ⍝ Time out - Insert code for housekeeping tasks here (deadlocks?)
     
          :Case 1010 ⍝ Object Not Found
              3 Log'Object ''',name,''' has been closed - RPC Server shutting down' ⋄ done←1
     
          :Else
              3 Log'Error in RPC.Wait: ',⍕wait
          :EndSelect
      :EndWhile
      ⎕DL 1 ⍝ Give responses time to complete
      {}##.DRC.Close name
      0 Log'Server ',name,' terminated.'     
            
      :If 2=⎕NC '#.AUTOSHUT'
      :AndIf 0≠#.AUTOSHUT
          ⎕OFF
      :EndIf
    ∇
    
    ∇ r←Open(cmd folder);i;Conn
      ⍝ Check whether a folder is serve-able 
     
      Conn←1↓⊃(cmd='.')⊂cmd
      
      :If (⊂folder)∊DBFolders
          r←0 'OK'
      :Else
          r←999 ('Database folder not found: ',folder)
      :EndIf
    ∇

    ∇ task←SetUser(cmd User);i;Conn
      ⍝ Return task ID
     
      Conn←1↓⊃(cmd='.')⊂cmd
     
      :If (⍴CONNS)<i←CONNS⍳⊂Conn
          3 Log'SetUser ',(⍕User),' for unknown connection ',Conn
      :Else
          0 Log'User set to ',(⍕User),' on connection ',Conn
          (i⊃USERS)←User
      :EndIf
     
      task←i⊃TASKS
    ∇

    ∇ Test;assert;START;resources;nprocesses;nresources;nevents;i;conns;conn;z;s
     ⍝ This should be a stand-alone test of vecdbsrv
     ⍝ Assumes existence of #.TestVecdbSrv

      assert←{'Assertion failed'⎕SIGNAL(⍵=0)/11}
      
      #.TestVecdbSrv.CreateTestConfig folder,'config.json'

      InitLocks 0
      LOGLEVEL←3 ⍝ Log everything
      MOCK←1     ⍝ Do not send CONGA messages
     
      Connect'.C1'
      assert(1 0)≡TASKS,USERS
      {}SetUser'.C1' 1234
      assert(1 1234)≡TASKS,USERS
     
      Connect'.C2'
      {}SetUser'.C2' 4321
     
      assert 0=Lock'.C1' '/ALLOC10' ⍝ Granted
      assert HELDBY≡,1             ⍝ Held by Task 1
      Release'.C1' '/ALLOC10'       ⍝ Release
      assert HELDBY≡,0             ⍝ Should now be free
     
      assert 0=Lock'.C1' '/ALLOC10' ⍝ Granted
      assert HELDBY≡,1             ⍝ Held by Task 1
      assert 1=Lock'.C2' '/ALLOC10' ⍝ Queued
      assert(2 '.C2')≡2⍴⊃QUEUES     ⍝ Task 2 is in the queue
     
      Release'.C1' '/ALLOC10'
      assert HELDBY≡,2             ⍝ Should now be held by Task 2
      assert 0=⊃⍴⊃QUEUES
     
      Disconnect'.C2'
      assert 1=⍴TASKS
      assert HELDBY≡,0             ⍝ Should now be free
      Disconnect'.C1'
      assert 0=⍴TASKS
     
     ⍝ --- performance test ---
     
      LOGLEVEL←3 ⍝ Erors only
     
      nprocesses←10
      nevents←1000×2×nprocesses
      ⎕←'Testing performance...'
      Connect¨conns←'.C'∘,¨⍕¨⍳nprocesses
      {}SetUser¨↓conns,[1.5]⍳nprocesses
      resources←nevents⍴('/BLAH/BLAH/ALLOC'∘,¨⍕¨⍳nprocesses),nprocesses⍴⊂'/BLAH/BLAH/ALLOC0'
     
      START←3⊃⎕AI
      :For i :In ⍳nprocesses+nevents
          conn←(1+nprocesses|i-1)⊃conns
          :If i≤nevents ⋄ z←Lock conn(i⊃resources) ⋄ :EndIf
          :If i>nprocesses ⋄ z←Release conn((i-nprocesses)⊃resources) ⋄ :EndIf
      :EndFor
     
      s←0.001×(3⊃⎕AI)-START
      ⎕←(⍕nevents),' released & locked in',(1⍕s),'s (',(,' '~⍨,'CI12'⎕FMT nevents÷s),' locks/s)'
    ∇

    assert←{'Assertion failed'⎕SIGNAL(⍵=0)/11}

:EndNamespace
