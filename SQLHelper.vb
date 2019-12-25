Imports System.Data
Imports System.Xml
Imports System.Data.SqlClient
Imports System.Collections

Public Class SQLHelper

    '--短鏈接，用完關閉鏈接--2015-05-23
    'Private Shared _Conn As New SqlConnection
    'Private Shared _ConnnectionString As String = "Data Source={0};Initial Catalog={1};User ID={2};Password={3};"
    Private Shared _ConnectionString As String = "Data Source={0};Initial Catalog={1};User ID={2};Password={3};MultipleActiveResultSets=true;"

#Region "--Propertys--"

    Public Shared Property ServerName As String = ""
    Public Shared Property Database As String = "yourdb"
    Public Shared Property UserName As String = "sa"
    Public Shared Property Password As String = "sa123"

#End Region

    'Public Shared Sub OpenDB()
    '    If _Conn.State = ConnectionState.Open Then Exit Sub
    '    _Conn.ConnectionString = GetConnnectionString()
    '    _Conn.Open()
    'End Sub

    'Public Shared Sub CloseDB()
    '    If _Conn.State = ConnectionState.Closed Then Exit Sub
    '    _Conn.Close()
    'End Sub

#Region "--Get Connection String--"

    Public Shared Function GetConnectionString() As String
        Return String.Format(_ConnectionString, ServerName, Database, UserName, Password)
    End Function

    Public Shared Function GetConnectionString(serverName As String, dataBase As String, userName As String, passWord As String) As String
        Return String.Format(_ConnectionString, serverName, dataBase, userName, passWord)
    End Function

#End Region

    Public Shared Function CreateConnection(connString As String) As SqlConnection
        Dim conn As New SqlClient.SqlConnection
        conn.ConnectionString = connString
        conn.Open()
        Return conn
    End Function

    Public Shared Function CreateConnection(ServerName As String, Database As String, UserName As String, Password As String) As SqlConnection
        Return CreateConnection(String.Format(_ConnectionString, ServerName, Database, UserName, Password))
    End Function

    Public Shared Function CreateCommand(strSql As String) As SqlCommand
        Dim cmd As New SqlClient.SqlCommand(sql)
        Return cmd
    End Function

    Public Shared Function TestLinkServer() As Boolean
        Using conn As New SqlClient.SqlConnection(GetConnectionString)
            conn.Open()
            If String.IsNullOrEmpty(GetSqlVersion(conn)) = False Then
                Return True
            Else
                Return False
            End If
        End Using
    End Function

    Public Shared Function TestLinkServer(ServerName As String, Database As String, UserName As String, Password As String) As Boolean
        Using conn As New SqlClient.SqlConnection(GetConnectionString(ServerName, Database, UserName, Password))
            conn.Open()
            If String.IsNullOrEmpty(GetSqlVersion(conn)) = False Then
                Return True
            Else
                Return False
            End If
        End Using
    End Function

    Private Shared Function GetSqlVersion(ByVal conn As SqlClient.SqlConnection) As String
        Dim rtn As String = String.Empty
        Using cmd As New SqlClient.SqlCommand("Master..xp_msver", conn)
            cmd.CommandType = CommandType.StoredProcedure
            Using reader As SqlClient.SqlDataReader = cmd.ExecuteReader
                While reader.Read()
                    If reader.Item("Name").ToString = "ProductVersion" Then
                        rtn = reader.Item("Character_Value").ToString
                        Exit While
                    End If
                End While
            End Using
        End Using
        Return rtn
    End Function

#Region "--Create SQL Object--"


    Public Shared Function AddParameter(cmd As SqlCommand, strParameterName As String, objValue As Object) As SqlCommand
        Return AddCmdParameter(cmd, strParameterName, objValue)
    End Function

    Public Shared Function AddCmdParameter(cmd As SqlCommand, strParameterName As String, objValue As Object) As SqlCommand
        If strParameterName.StartsWith("@") Then
            cmd.Parameters.Add(New SqlParameter(strParameterName, objValue))
        Else
            cmd.Parameters.Add(New SqlParameter("@" & strParameterName, objValue))
        End If
        Return cmd
    End Function

    Public Shared Function CreateNewConntion(serverName As String, dataBase As String, userName As String, passWord As String) As SqlClient.SqlConnection
        Return New SqlClient.SqlConnection(GetConnectionString(serverName, dataBase, userName, passWord))
    End Function

#End Region

#Region "--Get DataTable DataSet--"

    Public Shared Function GetRecordCount(ByVal sql As String) As Integer
        Using cmd As New SqlCommand(sql)
            Return GetRecordCount(cmd)
        End Using
    End Function

    Public Shared Function GetRecordCount(ByVal cmd As SqlCommand) As Integer
        Dim rtn As Integer = 0
        Dim obj As Object = ExecuteScalar(cmd)
        If obj IsNot Nothing AndAlso (Not obj.Equals(DBNull.Value)) Then
            rtn = Convert.ToInt32(obj)
        End If
        Return rtn
    End Function

    Public Shared Function GetDataTableByName(ByVal tblName As String) As DataTable
        Return GetDataTable("Select * From " & tblName)
    End Function

    Public Shared Function GetDataTable(ByVal sql As String) As DataTable
        Dim dt As New DataTable
        Using da As New SqlDataAdapter(sql, GetConnectionString)
            da.Fill(dt)
        End Using
        Return dt
    End Function

    Public Shared Function GetDataTable(ByVal cmd As SqlCommand) As DataTable
        Dim dt As New DataTable
        Using conn As New SqlConnection(GetConnectionString)
            Using da As New SqlDataAdapter(cmd)
                cmd.Connection = conn
                da.Fill(dt)
            End Using
        End Using
        Return dt
    End Function

    Public Shared Function GetDataTable(ByVal cmd As SqlCommand, conn As SqlConnection) As DataTable
        Dim dt As New DataTable
        Using da As New SqlDataAdapter(cmd)
            cmd.Connection = conn
            da.Fill(dt)
        End Using
        Return dt
    End Function

    Public Shared Function GetDataTableByOleDB(ByVal sql As String, ByVal connString As String) As DataTable
        Dim dt As New DataTable
        Using da As New System.Data.OleDb.OleDbDataAdapter(sql, connString)
            da.Fill(dt)
        End Using
        Return dt
    End Function

    Public Shared Function GetDataTableByOleDB(ByVal sql As String, ByVal oledbConn As System.Data.OleDb.OleDbConnection) As DataTable
        Dim dt As New DataTable
        Using da As New System.Data.OleDb.OleDbDataAdapter(sql, oledbConn)
            da.Fill(dt)
        End Using
        Return dt
    End Function

    Public Shared Function GetDataSet(ByVal sql As String, Optional ByVal ds As DataSet = Nothing) As DataSet
        If ds Is Nothing Then ds = New DataSet
        Return GetDataSet(sql, String.Empty, ds)
    End Function

    Public Shared Function GetDataSet(ByVal sql As String, ByVal tblName As String, Optional ByVal ds As DataSet = Nothing) As DataSet
        If ds Is Nothing Then ds = New DataSet
        Using cmd As New SqlCommand
            cmd.CommandText = sql
            If String.IsNullOrEmpty(tblName) Then
                Return GetDataSet(cmd, ds)
            Else
                Return GetDataSet(cmd, New String() {tblName}, ds)
            End If
        End Using
    End Function

    Public Shared Function GetDataSet(ByVal cmd As SqlCommand, Optional ByVal ds As DataSet = Nothing) As DataSet
        Return GetDataSet(cmd, New String() {}, ds)
    End Function

    Public Shared Function GetDataSet(ByVal cmd As SqlCommand, ByVal listTableNames() As String, Optional ByVal ds As DataSet = Nothing) As DataSet
        If ds Is Nothing Then ds = New DataSet
        Using conn As New SqlConnection(GetConnectionString)
            Using da As New SqlDataAdapter(cmd)
                cmd.Connection = conn
                da.Fill(ds)
                If ds.Tables.Count > 0 AndAlso listTableNames.Length > 0 Then
                    For i As Integer = 0 To listTableNames.Length - 1
                        ds.Tables(i).TableName = listTableNames(i)
                    Next
                End If
            End Using
        End Using
        Return ds
    End Function

    Public Shared Function GetSingleData(sql As String) As Object
        Using cmd As New SqlCommand
            cmd.CommandText = sql
            Return GetSingleData(cmd)
        End Using
    End Function

    Public Shared Function GetSingleData(cmd As SqlCommand) As Object
        Dim rtn As Object
        Using con As New SqlConnection(GetConnectionString)
            con.Open()
            cmd.Connection = con
            rtn = cmd.ExecuteScalar
        End Using
        If rtn IsNot Nothing AndAlso rtn IsNot DBNull.Value Then
            Return rtn
        Else
            Return Nothing
        End If
    End Function

#End Region

#Region "--Execute SQL Functions--"

    Public Shared Function ExecuteCommand(ByVal sql As String) As Integer
        Using con As New SqlConnection(GetConnectionString)
            con.Open()
            Using cmd As New SqlCommand(sql, con)
                Return cmd.ExecuteNonQuery()
            End Using
        End Using
    End Function

    Public Shared Function ExecuteCommand(ByVal sql As String, ByVal conn As SqlConnection) As Integer
        If conn.State <> ConnectionState.Open Then conn.Open()
        Using cmd As New SqlCommand(sql, conn)
            Return cmd.ExecuteNonQuery()
        End Using
    End Function

    Public Shared Function ExecuteCommand(ByVal cmd As SqlCommand) As Integer
        Using con As New SqlConnection(GetConnectionString)
            con.Open()
            cmd.Connection = con
            Return cmd.ExecuteNonQuery()
        End Using
    End Function

    Public Shared Function ExecuteCommand(ByVal cmd As SqlCommand, conn As SqlConnection) As Integer
        If conn.State <> ConnectionState.Open Then conn.Open()
        cmd.Connection = conn
        Return cmd.ExecuteNonQuery()
    End Function

    Public Shared Function ExecuteOLEDBCommand(ByVal sql As String, ByVal OledbConn As System.Data.OleDb.OleDbConnection) As Integer
        Using cmd As New System.Data.OleDb.OleDbCommand(sql, OledbConn)
            If OledbConn.State <> ConnectionState.Open Then OledbConn.Open()
            Return cmd.ExecuteNonQuery()
        End Using
    End Function

    Public Shared Function ExecuteOLEDBCommand(ByVal cmd As System.Data.OleDb.OleDbCommand, ByVal OledbConn As System.Data.OleDb.OleDbConnection) As Integer
        cmd.Connection = OledbConn
        Return cmd.ExecuteNonQuery()
    End Function

    Public Shared Function ExecuteScalar(sql As String) As Object
        Using cmd As New SqlCommand
            cmd.CommandText = sql
            Return ExecuteScalar(cmd)
        End Using
    End Function

    Public Shared Function ExecuteScalar(cmd As SqlCommand) As Object
        Using con As New SqlConnection(GetConnectionString)
            con.Open()
            cmd.Connection = con
            Return cmd.ExecuteScalar
        End Using
    End Function

#End Region

#Region "--Check Record Functions--"

    Public Shared Function GetRecordStatus(ByVal tblName As String, ByVal strKeyColName As String, ByVal strKeyColValue As String) As String
        Dim rtn As String = String.Empty
        Using cmd As New SqlCommand
            cmd.CommandText = String.Format("select strStatus From {0} Where {1}=@P1", tblName, strKeyColName)
            cmd.Parameters.Add(New SqlParameter("@P1", strKeyColValue))
            Using tbl As DataTable = GetDataTable(cmd)
                If tbl IsNot Nothing AndAlso tbl.Rows.Count > 0 Then
                    rtn = tbl.Rows(0).Item(0).ToString
                End If
            End Using
        End Using
        Return rtn
    End Function

    Public Shared Function GetRecordStatus(ByVal SelectCmd As SqlCommand) As String
        Dim rtn As String = String.Empty
        Using cmd As SqlCommand = SelectCmd
            Using tbl As DataTable = GetDataTable(cmd)
                If tbl IsNot Nothing AndAlso tbl.Rows.Count > 0 Then
                    rtn = tbl.Rows(0).Item(0).ToString
                End If
            End Using
        End Using
        Return rtn
    End Function

    Public Shared Function CheckRecordKeyWordNoConflict4Update(ByVal tblName As String, ByVal strIDColName As String, ByVal lngIDColID As Long, ByVal strKeyColName As String, ByVal strKeyColNewName4Update As String) As Boolean
        Using cmd As New SqlCommand
            With cmd
                .CommandText = String.Format("Select 1 From {0} Where {1}<>@P1 And {2}=@P2", tblName, strIDColName, strKeyColName)
                .Parameters.Add(New SqlParameter("@P1", lngIDColID))
                .Parameters.Add(New SqlParameter("@P2", strKeyColNewName4Update))
            End With
            Return CheckRecordExist(cmd)
        End Using
    End Function

    Public Shared Function CheckRecordExist(ByVal strSql As String) As Boolean
        Using cmd As New SqlCommand
            cmd.CommandText = strSql
            Return CheckRecordExist(cmd)
        End Using
    End Function

    Public Shared Function CheckRecordExist(ByVal cmd As SqlCommand) As Boolean
        Dim rtn As Boolean = False
        Using tbl As DataTable = GetDataTable(cmd)
            If tbl IsNot Nothing AndAlso tbl.Rows.Count > 0 Then
                rtn = True
            End If
        End Using
        Return rtn
    End Function

    Public Shared Function CheckRecordExist(ByVal tblName As String, ByVal KeyColName As String, ByVal KeyColValue As String) As Boolean
        Using cmd As New SqlCommand
            With cmd
                .CommandText = String.Format("Select 1 From {0} Where {1}=@P1", tblName, KeyColName)
                .Parameters.Add(New SqlParameter("@P1", KeyColValue))
            End With
            Return CheckRecordExist(cmd)
        End Using
    End Function

#End Region

#Region "--Other Helper Operate--"

    Public Shared Function GetDBIdentity(ByVal tblName As String) As Long
        Dim row As DataRow
        'row = GetDataTable("select @@IDENTITY as DBIdentity").Rows(0)
        row = GetDataTable("select IDENT_CURRENT('" & tblName & "') as DBIdentity").Rows(0)
        If IsDBNull(row("DBIdentity")) Then
            Return -1
        Else
            Return row("DBIdentity")
        End If
    End Function

    Public Shared Sub CleanTable(ByVal tblName As String)
        ', Optional ByVal blnResetID As Boolean = False)
        'ExecuteCommand("delete from " & tblName)s
        'If blnResetID Then ExecuteCommand("DBCC CHECKIDENT(" & tblName & ", reseed, 0)")
        ExecuteCommand("truncate table " & tblName)
    End Sub

#End Region

    Public Class Builder
        Implements System.IDisposable

        Private cmd As New SqlClient.SqlCommand
        Private mWhereList As New List(Of String)

        Public Property SqlCDKey As String = " And "
        Public Property SortBy As String = ""
        Public Property GroudBy As String = ""
        Private mWhere As String = " Where "

        Public Sub New()

        End Sub

        Public Sub New(ByVal strSQL As String)
            cmd.CommandText = strSQL
        End Sub

        Public Function SetSQL(ByVal strSQL As String) As Builder
            cmd.CommandText = strSQL
            Return Me
        End Function

        Public Function GetCommand() As SqlClient.SqlCommand
            Return cmd
        End Function

        Public Function GetCommandText() As String
            Return cmd.CommandText
        End Function

        Public Function Clear() As Builder
            mWhereList.Clear()
            cmd.Parameters.Clear()
            cmd.CommandText = String.Empty
            cmd.CommandType = CommandType.Text
            SortBy = ""
            GroudBy = ""
            Return Me
        End Function

        Public Function AddNormalParameter(ByVal strWhereParameter As String, ByVal objValue As Object) As Builder
            AddWhere(strWhereParameter & "=@" & strWhereParameter)
            AddParameter(strWhereParameter, objValue)
            Return Me
        End Function

        Public Function AddWhere(ByVal strWhere As String) As Builder
            If String.IsNullOrEmpty(strWhere) Then showIsEmptyException()
            mWhereList.Add(strWhere)
            Return Me
        End Function

        Public Function AddParameter(ByVal strParameterName As String, ByVal objValue As Object) As Builder
            If String.IsNullOrEmpty(strParameterName) Then showIsEmptyException()
            If strParameterName.StartsWith("@") Then
                cmd.Parameters.Add(New SqlParameter(strParameterName, objValue))
            Else
                cmd.Parameters.Add(New SqlParameter("@" & strParameterName, objValue))
            End If
            Return Me
        End Function

        Public Function AddWhereAndParameter(ByVal strWhere As String, ByVal strParameter As String, ByVal objValue As Object) As Builder
            AddWhere(strWhere)
            AddParameter(strParameter, objValue)
            Return Me
        End Function

        Public ReadOnly Property GetWhereString As String
            Get
                Dim strWhere As String = String.Empty
                If mWhereList.Count > 0 Then
                    For Each strCDKey As String In mWhereList
                        strWhere &= (IIf(String.IsNullOrEmpty(strWhere), "", SqlCDKey) & strCDKey)
                    Next
                End If
                Return strWhere
            End Get
        End Property

        Public ReadOnly Property GetWhereFullString As String
            Get
                Dim strWhere As String = GetWhereString
                If Not String.IsNullOrEmpty(strWhere) Then Return mWhere & strWhere
                Return strWhere
            End Get
        End Property

        Private Function getCmd4Run() As Boolean
            If String.IsNullOrEmpty(Me.cmd.CommandText) Then
                showIsEmptyException()
                Return False
            End If
            If mWhereList.Count > 0 Then Me.cmd.CommandText &= GetWhereFullString
            If Not String.IsNullOrEmpty(Me.GroudBy) Then Me.cmd.CommandText &= Me.GroudBy
            If Not String.IsNullOrEmpty(Me.SortBy) Then Me.cmd.CommandText &= Me.SortBy
            Return True
        End Function

        Public Function GetRecordCount() As Integer
            If getCmd4Run() Then Return SQLHelper.GetRecordCount(Me.GetCommand)
            Return -1
        End Function

        Public Function GetRecordCount(ByVal strSql As String) As Integer
            Return SQLHelper.GetRecordCount(Me.SetSQL(strSql).GetCommand)
        End Function

        Public Function ExeCommand() As Integer
            If getCmd4Run() Then Return SQLHelper.ExecuteCommand(Me.GetCommand)
            Return -1
        End Function

        Public Function GetDataTable() As DataTable
            If getCmd4Run() Then Return SQLHelper.GetDataTable(Me.GetCommand)
            Return Nothing
        End Function

        Public Function GetSingleData() As Object
            If getCmd4Run() Then Return SQLHelper.GetSingleData(Me.GetCommand)
            Return Nothing
        End Function

        Public Function IsStoredProcedure() As Builder
            cmd.CommandType = CommandType.StoredProcedure
            Return Me
        End Function

        Public Function IsTableName() As Builder
            cmd.CommandType = CommandType.TableDirect
            Return Me
        End Function

        Public Function Build() As SqlClient.SqlCommand
            getCmd4Run()
            Return cmd
        End Function

        Private Sub showIsEmptyException()
            MessageBox.Show("请设置SQL查询语句、查询条件、查询参数等内容！")
        End Sub

#Region "IDisposable Support"

        Private disposedValue As Boolean

        Protected Overridable Sub Dispose(disposing As Boolean)
            If Not Me.disposedValue Then
                If disposing Then

                    If cmd IsNot Nothing Then
                        cmd.Dispose()
                        cmd = Nothing
                    End If
                End If
            End If
            Me.disposedValue = True
        End Sub

        Public Sub Dispose() Implements IDisposable.Dispose
            Dispose(True)
            GC.SuppressFinalize(Me)
        End Sub

#End Region

        Protected Overrides Sub Finalize()
            MyBase.Finalize()
            Dispose()
        End Sub

    End Class

End Class
