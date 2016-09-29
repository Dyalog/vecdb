:Class APLProcess
    ⍝ Start (and eventually dispose of) a Process

    (⎕IO ⎕ML)←1 1

    :Field Public Args←''
    :Field Public Ws←''
    :Field Public Exe←''
    :Field Public Proc←⎕NS ''
    :Field Public onExit←''
    :Field Public RunTime←0    ⍝ Boolean or name of runtime executable 
    :Field Public IsWin←0
    :Field Public IsSsh←0

    :Field Public RIDE_INIT←'' ⍝ RIDE parameters if remote debugging is to be allowed

    endswith←{w←,⍵ ⋄ a←,⍺ ⋄ w≡(-(⍴a)⌊⍴w)↑a}
    tonum←{⊃⊃(//)⎕VFI ⍵}
    eis←{2>|≡⍵:,⊂⍵ ⋄ ⍵} ⍝ enclose if simple

    ∇ path←SourcePath;source
    ⍝ Determine the source path of the class
      
      :Trap 6
         source←⍎'(⊃⊃⎕CLASS ⎕THIS).SALT_Data.SourceFile' ⍝ ⍎ works around a bug
      :Else
          source←(⊃⊃⎕CLASS ⎕THIS).SALT_Data.SourceFile
          :If 0=⍴source←{((⊃¨⍵)⍳⊃⊃⎕CLASS ⎕THIS)⊃⍵,⊂''}5177⌶⍬
              source←⎕WSID
          :Else ⋄ source←4⊃source
          :EndIf
      :EndTrap
      path←{(-⌊/(⌽⍵)⍳'\/')↓⍵}source
    ∇

    ∇ make1 args;rt;cmd;ws
      :Access Public Instance
      :Implements Constructor
      ⍝ args is:
      ⍝  [1]  the workspace to load
      ⍝  [2]  any command line arguments
      ⍝ {[3]} if present, a Boolean indicating whether to use the runtime version, OR a character vector of the executable name to run
      args←{2>|≡⍵:,⊂⍵ ⋄ ⍵}args
      args←3↑args,(⍴args)↓'' '' 0
      (ws cmd rt)←args   
      PATH←SourcePath
      Start(ws cmd rt)  
    ∇

    ∇ Run
      :Access Public Instance
      Start(Ws Args RunTime)
    ∇

    ∇ Start(ws args rt);psi;pid
      (Ws Args)←ws args
      :If 0≠⍴RIDE_INIT
          args←args,' RIDE_SPAWNED=1 RIDE_INIT=',RIDE_INIT
      :EndIf
     
      :If ~0 2∊⍨10|⎕DR rt ⍝ if rt is character, it's the executable name
          Exe←(RunTimeName⍣rt)GetCurrentExecutable
      :Else
          Exe←rt
          rt←0
      :EndIf
   ⍝   ws,←rt/' salt'  ⍝ if runtime, load the salt workspace first, which will subsequently load the target workspace
      :If IsWin←IsWindows
          ⎕USING←'System,System.dll'
          psi←⎕NEW Diagnostics.ProcessStartInfo,⊂Exe(ws,' ',args)
          psi.WindowStyle←Diagnostics.ProcessWindowStyle.Minimized
          Proc←Diagnostics.Process.Start psi
      :Else ⍝ Unix     
          :If IsSsh←326=⎕DR Exe
              Proc←SshProc Exe
          :Else
          pid←_SH'{ ',args,' ',Exe,' +s ',ws,' -c APLppid=',(⍕GetCurrentProcessId),' </dev/null >/dev/null 2>&1 & } ; echo $!'
          Proc.Id←pid
          Proc.HasExited←HasExited
          :EndIf
          Proc.StartTime←⎕NEW Time ⎕TS
      :EndIf
    ∇

    ∇ Close;count;limit
      :Implements Destructor
      WaitForKill&200 0.1 ⍝ Start a new thread to do the dirty work
    ∇

    ∇ WaitForKill(limit interval);count
      :If (0≠⍴onExit)∧~HasExited ⍝ If the process is still alive
          :Trap 0 ⋄ ⍎onExit ⋄ :EndTrap ⍝ Try this
     
          count←0
          :While ~HasExited
              {}⎕DL interval
              count←count+1
          :Until count>limit
      :EndIf ⍝ OK, have it your own way
     
      {}Kill Proc
    ∇

    ∇ r←IsWindows
      :Access Public Shared
      r←'Win'≡3↑⎕IO⊃#.⎕WG'APLVersion'
    ∇

    ∇ r←GetCurrentProcessId;t
      :Access Public Shared
      :If IsWin
          r←⍎'t'⎕NA'U4 kernel32|GetCurrentProcessId'
      :ElseIf IsSsh
          ∘∘∘
          r←Proc.Pid
      :Else
          r←tonum⊃_SH'echo $PPID'
      :EndIf
    ∇

    ∇ r←GetCurrentExecutable;⎕USING;t;gmfn
      :Access Public Shared
      :If IsWin
          r←''
          :Trap 0
              'gmfn'⎕NA'U4 kernel32|GetModuleFileName* P =T[] U4'
              r←⊃⍴/gmfn 0(1024⍴' ')1024
          :EndTrap
          :If 0∊⍴r
              ⎕USING←'System,system.dll'
              r←2 ⎕NQ'.' 'GetEnvironment' 'DYALOG'
              r←r,(~(¯1↑r)∊'\/')/'/' ⍝ Add separator if necessary
              r←r,(Diagnostics.Process.GetCurrentProcess.ProcessName),'.exe'
          :EndIf 
      :ElseIf IsSsh
          ∘∘∘ ⍝ Not supported
      :Else
          t←⊃_PS'-o args -p ',⍕GetCurrentProcessId ⍝ AWS
          :If '"'''∊⍨⊃t  ⍝ if command begins with ' or "
              r←{⍵/⍨{∧\⍵∨≠\⍵}⍵=⊃⍵}t
          :Else
              r←{⍵↑⍨¯1+1⍳⍨(¯1↓0,⍵='\')<⍵=' '}t ⍝ otherwise find first non-escaped space (this will fail on files that end with '\\')
          :EndIf
      :EndIf
    ∇

    ∇ r←RunTimeName exe
    ⍝ Assumes that:
    ⍝ Windows runtime ends in "rt.exe"
    ⍝ *NIX runtime ends in ".rt"
      r←exe
      :If IsWin
          :If 'rt.exe'≢¯6↑{('rt.ex',⍵)[⍵⍳⍨'RT.EX',⍵]}exe ⍝ deal with case insensitivity
              r←'rt.exe',⍨{(~∨\⌽<\⌽'.'=⍵)/⍵}exe
          :EndIf
      :Else
          r←exe,('.rt'≢¯3↑exe)/'.rt'
      :EndIf
    ∇


    ∇ r←KillChildren Exe;kids;⎕USING;p;m;i;mask
      :Access Public Shared
      ⍝ returns [;1] pid [;2] process name of any processes that were not killed
      r←0 2⍴0 ''
      :If ~0∊⍴kids←ListProcesses Exe ⍝ All child processes using the exe
          :If IsWin
              ⎕USING←'System,system.dll'
              p←Diagnostics.Process.GetProcessById¨kids[;1]
              p.Kill
              ⎕DL 1
              :If 0≠⍴p←(~p.HasExited)/p
                  ⎕DL 1
                  p.Kill
                  ⎕DL 1
                  :If ∨/m←~p.HasExited
                      r←(kids[;1]∊m/p.Id)⌿kids
                  :EndIf
              :EndIf
          :ElseIf IsSsh
              ∘∘∘
          :Else
              mask←(⍬⍴⍴kids)⍴0
              :For i :In ⍳⍴mask
                  mask[i]←Shoot kids[i;1]
              :EndFor
              r←(~mask)⌿kids
          :EndIf
      :EndIf
    ∇

    ∇ r←{all}ListProcesses procName;me;⎕USING;procs;unames;names;name;i;pn;kid;parent;mask;n
      :Access public shared
    ⍝ returns either my child processes or all processes
    ⍝ procName is either '' for all children, or the name of a process
    ⍝ r[;1] - child process number (Id)
    ⍝ r[;2] - child process name
      me←GetCurrentProcessId
      r←0 2⍴0 ''
      procName←,procName
      all←{6::⍵ ⋄ all}0 ⍝ default to just my childen
     
      :If IsWin
          ⎕USING←'System,system.dll'
     
          :If 0∊⍴procName ⋄ procs←Diagnostics.Process.GetProcesses''
          :Else ⋄ procs←Diagnostics.Process.GetProcessesByName⊂procName ⋄ :EndIf
          :If all
              r←↑procs.(Id ProcessName)
              r⌿⍨←r[;1]≠me
          :Else
              :If 0<⍴procs
                  unames←∪names←procs.ProcessName
                  :For name :In unames
                      :For i :In ⍳n←1+.=(,⊂name)⍳names
                          pn←name,(n≠1)/'#',⍕i
                          :Trap 0 ⍝ trap here just in case a process disappeared before we get to it
                              parent←⎕NEW Diagnostics.PerformanceCounter('Process' 'Creating Process Id'pn)
                              :If me=parent.NextValue
                                  kid←⎕NEW Diagnostics.PerformanceCounter('Process' 'Id Process'pn)
                                  r⍪←(kid.NextValue)name
                              :EndIf
                          :EndTrap
                      :EndFor
                  :EndFor
              :EndIf
          :EndIf 
      :ElseIf IsSsh
          ∘∘∘
      :Else ⍝ Linux
      ⍝ unfortunately, Ubuntu (and perhaps others) report the PPID of tasks started via ⎕SH as 1
      ⍝ so, the best we can do at this point is identify processes that we tagged with ppid=
          mask←' '∧.=procs←' ',↑_PS'-eo pid,cmd',((~all)/' | grep APLppid=',(⍕GetCurrentProcessId)),(0<⍴procName)/' | grep ',procName,' | grep -v grep' ⍝ AWS
          mask∧←2≥+\mask
          procs←↓¨mask⊂procs
          mask←me≠tonum¨1⊃procs ⍝ remove my task
          procs←mask∘/¨procs[1 2]
          mask←1
          :If 0<⍴procName
              mask←∨/¨(procName,' ')∘⍷¨(2⊃procs),¨' '
          :EndIf
          mask>←∨/¨'grep '∘⍷¨2⊃procs ⍝ remove procs that are for the searches
          procs←mask∘/¨procs
          r←↑[0.1]procs
      :EndIf
    ∇

    ∇ r←Kill;delay
      :Access Public Instance
      r←0 ⋄ delay←0.1
      :Trap 0
          :If IsWin
              Proc.Kill
              :Repeat
                  ⎕DL delay
                  delay+←delay
              :Until (delay>10)∨Proc.HasExited
          :ElseIf IsSsh
              ∘∘∘
          :Else ⍝ Local UNIX
              {}UNIXIssueKill 3 Proc.Id ⍝ issue strong interrupt
              {}⎕DL 2 ⍝ wait a couple seconds for it to react
              :If ~Proc.HasExited←~UNIXIsRunning Proc.Id
                  {}UNIXIssueKill 9 Proc.Id ⍝ issue strong interrupt
                  {}⎕DL 2 ⍝ wait a couple seconds for it to react
              :AndIf ~Proc.HasExited←~UNIXIsRunning Proc.Id
                  :Repeat
                      ⎕DL delay
                      delay+←delay
                  :Until (delay>10)∨Proc.HasExited~UNIXIsRunning Proc.Id
              :EndIf
          :EndIf
          r←Proc.HasExited
      :EndTrap
    ∇

    ∇ r←Shoot Proc;MAX;res
      MAX←100
      r←0
      :If 0≠⎕NC⊂'Proc.HasExited'
          :Repeat
              :If ~Proc.HasExited
                  :If IsWin
                      Proc.Kill
                      ⎕DL 0.2
                  :ElseIf IsSsh
                      ∘∘∘
                  :Else
                      {}UNIXIssueKill 3 Proc.Id ⍝ issue strong interrupt AWS
                      {}⎕DL 2 ⍝ wait a couple seconds for it to react
                      :If ~Proc.HasExited←0∊⍴res←UNIXGetShortCmd Proc.Id       ⍝ AWS
                          Proc.HasExited∨←∨/'<defunct>'⍷⊃,/res
                      :EndIf
                  :EndIf
              :EndIf
              MAX-←1
          :Until Proc.HasExited∨MAX≤0
          r←Proc.HasExited
      :ElseIf 2=⎕NC'Proc' ⍝ just a process id?
          {}UNIXIssueKill 9 Proc.Id
          {}⎕DL 2
          r←~UNIXIsRunning Proc.Id  ⍝ AWS
      :EndIf
    ∇

    ∇ r←HasExited
      :Access public instance
      :If IsWin
          r←{0::⍵ ⋄ Proc.HasExited}1 
      :ElseIf IsSsh
          ∘∘∘
      :Else
          r←~UNIXIsRunning Proc.Id ⍝ AWS
      :EndIf
    ∇

    ∇ r←IsRunning args;⎕USING;start;exe;pid;proc;diff;res
      :Access public shared
      ⍝ args - pid {exe} {startTS}
      r←0
      args←eis args
      (pid exe start)←3↑args,(⍴args)↓0 ''⍬
      :If IsWin
          ⎕USING←'System,system.dll'
          :Trap 0
              proc←Diagnostics.Process.GetProcessById pid
              r←1
          :Else
              :Return
          :EndTrap
          :If ''≢exe
              r∧←exe≡proc.ProcessName
          :EndIf
          :If ⍬≢start
              :Trap 90
                  diff←|-/#.DFSUtils.DateToIDN¨start(proc.StartTime.(Year Month Day Hour Minute Second Millisecond))
                  r∧←diff≤24 60 60 1000⊥0 1 0 0÷×/24 60 60 1000 ⍝ consider it a match within a 1 minute window
              :Else
                  r←0
              :EndTrap
          :EndIf
      :ElseIf IsSsh
          ∘∘∘
      :Else
          r←UNIXIsRunning pid
      :EndIf
    ∇

    ∇ r←Stop pid;proc
      :Access public shared
    ⍝ attempts to stop the process with processID pid
      :If IsWin
          ⎕USING←'System,system.dll'
          :Trap 0
              proc←Diagnostics.Process.GetProcessById pid
          :Else
              r←1
              :Return
          :EndTrap
          proc.Kill
          {}⎕DL 0.5
          r←~##.APLProcess.IsRunning pid
      :ElseIf IsSsh
          ∘∘∘
      :ElseIf
          {}UNIXIssueKill 3 pid ⍝ issue strong interrupt
      :EndIf
    ∇

    ∇ r←UNIXIsRunning pid;txt
    ⍝ Return 1 if the process is in the process table and is not a defunct
      r←0
      →(r←' '∨.≠txt←UNIXGetShortCmd pid)↓0
      r←~∨/'<defunct>'⍷txt
    ∇

    ∇ {r}←UNIXIssueKill(signal pid)
      signal pid←⍕¨signal pid
      cmd←'kill -',signal,' ',pid,' >/dev/null 2>&1 ; echo $?'
      :If IsSsh
        ∘∘∘
      :Else
      r←⎕SH cmd
      :EndIf
    ∇

    ∇ r←UNIXGetShortCmd pid;cmd
      ⍝ Retrieve sort form of cmd used to start process <pid> 
      cmd←'ps -o cmd -p ',(⍕pid),' 2>/dev/null ; exit 0' 
      :If IsSsh
          ∘∘∘
      :Else
      r←⊃1↓⎕SH cmd
      :EndIf
    ∇

    ∇ r←_PS cmd;ps
      ps←'ps ',⍨('AIX'≡3↑⊃'.'⎕WG'APLVersion')/'/usr/sysv/bin/'    ⍝ Must use this ps on AIX
      r←1↓⎕SH ps,cmd,' 2>/dev/null; exit 0'                  ⍝ Remove header line
    ∇

    ∇ r←{quietly}_SH cmd
      :Access public shared
      quietly←{6::⍵ ⋄ quietly}0
      :If quietly
          cmd←cmd,' </dev/null 2>&1'
      :EndIf
      r←{0::'' ⋄ ⎕SH ⍵}cmd
    ∇

    :Class Time
        :Field Public Year
        :Field Public Month
        :Field Public Day
        :Field Public Hour
        :Field Public Minute
        :Field Public Second
        :Field Public Millisecond

        ∇ make ts
          :Implements Constructor
          :Access Public
          (Year Month Day Hour Minute Second Millisecond)←7↑ts
          ⎕DF(⍕¯2↑'00',⍕Day),'-',((12 3⍴'JanFebMarAprMayJunJulAugSepOctNovDec')[⍬⍴Month;]),'-',(⍕100|Year),' ',1↓⊃,/{':',¯2↑'00',⍕⍵}¨Hour Minute Second
        ∇

    :EndClass

    ∇ r←ProcessUsingPort port;t
    ⍝ return the process ID of the process (if any) using a port
      :Access public shared
      r←⍬
      :If IsWin
          :If ~0∊⍴t←_SH'netstat -a -n -o'
          :AndIf ~0∊⍴t/⍨←∨/¨'LISTENING'∘⍷¨t
          :AndIf ~0∊⍴t/⍨←∨/¨((':',⍕port),' ')∘⍷¨t
              r←∪∊¯1↑¨(//)∘⎕VFI¨t
          :EndIf
      :Else
          :If ~0∊⍴t←_SH'netstat -l -n -p 2>/dev/null | grep '':',(⍕port),' '''
              r←∪∊{⊃(//)⎕VFI{(∧\⍵∊⎕D)/⍵}⊃¯1↑{⎕ML←3 ⋄ (' '≠⍵)⊂⍵}⍵}¨t
          :EndIf
      :EndIf
    ∇

    ∇ r←MyDNSName;GCN
      :Access Public Shared
     
      :If IsWin
          'GCN'⎕NA'I4 Kernel32|GetComputerNameEx* U4 >0T =U4'
          r←2⊃GCN 7 255 255
          :Return
      ⍝ ComputerNameNetBIOS = 0
      ⍝ ComputerNameDnsHostname = 1
      ⍝ ComputerNameDnsDomain = 2
      ⍝ ComputerNameDnsFullyQualified = 3
      ⍝ ComputerNamePhysicalNetBIOS = 4
      ⍝ ComputerNamePhysicalDnsHostname = 5
      ⍝ ComputerNamePhysicalDnsDomain = 6
      ⍝ ComputerNamePhysicalDnsFullyQualified = 7 <<<
      ⍝ ComputerNameMax = 8
      :ElseIf IsSsh
          ∘∘∘ ⍝ Not supported
      :ElseIf
          r←⊃_SH'hostname'
      :EndIf
    ∇

    ∇ Proc←SshProc(host user keyfile cmd);conn;z;kf;allpids;guid;listpids;pids;⎕USING;pid;tid
      ⎕USING←'Renci.SshNet,',PATH,'/Renci.SshNet.dll'
      kf←⎕NEW PrivateKeyFile (,⊂keyfile)      
      conn←⎕NEW SshClient (host 22 user (,kf)) 

      :Trap 0
          conn.Connect    ⍝ This is defined to be a void()
      :Case 90 ⋄ ('Error creating ssh client instance: ',⎕EXCEPTION.Message) ⎕SIGNAL 11
      :Else ⋄ 'Unexpected error creating ssh client instance' ⎕SIGNAL 11
      :EndTrap 
      
      listpids←{0~⍨2⊃(⎕UCS 10)⎕VFI (conn.RunCommand ⊂'ps -u ',user,' | grep dyalog | grep -v grep | awk ''{print $2}''').Result}
      guid←'dyalog-ssh-',(⍕⎕TS)~' '
      pids←listpids ⍬
      tid←conn.RunCommand&⊂cmd ⍝ ,' -c ''',guid,'''' 
      :If 1=⍴pid←(listpids ⍬)~pids ⋄ pid←⊃pid
      :Else ⋄ ∘∘∘ ⋄ :EndIf ⍝ failed to start  
      Proc←⎕NS ''
      Proc.SshConn←conn
      Proc.tid←tid
      Proc.Pid←

    ∇

:EndClass
