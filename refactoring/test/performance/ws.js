var server = require( 'http' ).createServer();

// our html template
var script = function( f ) {
	// get just the content of this script
	var s = /^[^\{]+\{\n?([\s\S]+)\}/.exec( f )[1];
	return [
		'<!DOCTYPE html><html>',
		'<head>',
		'<script type="text/javascript">',s,'</script>',
		'</head>',
		'<body style="font-size:40px"></body>',
		'</html>'
	].join( '\n' );
}

// some browser side javascript
var makeWebSocket = ( function() {
	var ws = new WebSocket( 'ws://localhost:3000' );
	var start;
	ws.addEventListener( 'open', function() {
		start = new Date().getTime();
		this.send( '.' );
	} )
	var count = 0;
	ws.addEventListener( 'message', function( e ) {
		var end = new Date().getTime();
		var diff = String( end - start )
		if( diff >= 1000 ) {
			alert( 'Your browser can route'+count+'websocket requests per second' );
		}
		else {
			this.send( e.data );
		}
		count++;
	} );
}  )

// send our page
server.addListener( 'request', function( req, res ) {
	res.writeHead( 200 );
	res.end( script( makeWebSocket ) );
} )

// create the most basic websocket imaginable
server.addListener( 'upgrade', function( req, socket ) {
	socket.send = function( s ) {
		this.write( '\u0000', 'binary' );
		this.write( s, 'utf8' );
		this.write( '\uffff', 'binary' );
	}
	socket.write( [
		'HTTP/1.1 101 Web Socket Protocol Handshake',
		'Upgrade: WebSocket',
		'Connection: Upgrade',
		'WebSocket-Origin: '+req.headers.origin,
		'WebSocket-Location: ws://'+req.headers.host+'/',
		null, null
	].join( "\r\n" ) );
	socket.addListener( 'data', function( s ) {
		var data = s.toString( 'utf8' ).slice( 1, -1 );
		this.send( data );
	} );
} )

server.listen( 8020 );