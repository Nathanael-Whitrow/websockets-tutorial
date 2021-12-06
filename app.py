#!/usr/bin/env python

import asyncio
import itertools
import json
import secrets
import websockets

from connect4 import Connect4
from connect4 import PLAYER1, PLAYER2

# Store connections and games in global memory
JOIN = {}
WATCH = {}

async def error(websocket, message):
    """
    Send an error message

    """
    event = {
        "type": "error",
        "message": message,
    }
    await websocket.send(json.dumps(event))

async def replay(websocket, game):
    """
    Send previous moves
    Useful for gettin a player up to speed

    """
    # Make a copy to avoid an exception if game.moves
    # changes while iteration is in progress. If a move is
    # played while replay is running, moves will be sent
    # out of order but each move will be sent once and eventually
    # the UI will be consistent
    for player, column, row in game.moves.copy():
        event = {
            "type": "play",
            "player": player,
            "column": column,
            "row": row,
        }
        await websocket.send(json.dumps(event))

async def play(websocket, game, player, connected):
    """
    Receive and process moves from a player

    """
    async for message in websocket:
        # Parse "play" event from UI
        event = json.loads(message)
        assert event["type"] == "play"
        column = event["column"]

        try:
            # Play the game
            row = game.play(player, column)
        except RuntimeError as exc:
            # Send an error event if the move was illegal
            # and wait for the next event
            await error(websocket, str(exc))
            continue
        
        # Send a "play" event to update the UI
        event = {
            "type": "play",
            "player": player,
            "column": column,
            "row": row,
        }
        websockets.broadcast(connected, json.dumps(event))
        
        # If move is winning, send a 'win' event
        if game.winner is not None:
            event = {
                "type": "win",
                "player": player,
            }
            websockets.broadcast(connected, json.dumps(event))

async def start(websocket):
    """
    Handle a connection from the first player: start a new game.

    """
    # Initialize a Connect Four game,
    # the set of WebSocket connections receiving moves
    # from this game, and secret access token.
    game = Connect4()
    connected = {websocket}

    join_key = secrets.token_urlsafe(12)
    JOIN[join_key] = game, connected

    watch_key = secrets.token_urlsafe(12)
    WATCH[watch_key] = game, connected

    # Keep global memory clean with try blocks
    try:
        # Send the secret access toekn to the browser
        # of the first player, where it will be used for
        # build a "join" link.
        event = {
            "type": "init",
            "join": join_key,
            "watch": watch_key,
        }
        await websocket.send(json.dumps(event))
        # Receive and process moves from the first player
        await play(websocket, game, PLAYER1, connected)
    finally:
        del JOIN[join_key]

async def join(websocket, join_key):
    # Find the Connect Four game.
    try:
        game, connected = JOIN[join_key]
    except KeyError:
        await error(websocket, "Game not found.")
        return
    
    # Register to receive moves from this game.
    connected.add(websocket)
    try:
        # Send the first move, in case the first player
        # already played it.
        await replay(websocket, game)
        # Receive and process moves from the second player
        await play(websocket, game, PLAYER2, connected)
    finally:
        connected.remove(websocket)

async def watch(websocket, watch_key):
    """
    Handle a connection from a spectator: watch an existing game.

    """
    # Find the Connect Four game.
    try:
        game, connected = WATCH[watch_key]
    except KeyError:
        await error(websocket, "Game not found.")
        return
    
    # Register to receive moves from this game.
    connected.add(websocket)
    try:
        # Send previous moves to update spectators
        await replay(websocket, game)
        # Wait until game is over
        await websocket.wait_closed()
    finally:
        connected.remove(websocket)

async def handler(websocket, path):
    """
    Handle a connection and dispatch it according to who is connecting.

    """
    # Receive and parse the "init" event from the UI.
    message = await websocket.recv()
    event = json.loads(message)
    assert event["type"] == "init"

    # Split incoming websocket connections off
    if "join" in event:
        # Second player joins an existing game.
        await join(websocket, event["join"])
    elif "watch" in event:
        # Specator watches an existing game
        await watch(websocket, event["watch"])
    else:
        # First player starts a new game
        await start(websocket)


async def main():
    # Set the stop condition when receiving SIGTERM
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)

    port = int(os.environ.get("PORT", "8001"))
    async with websockets.serve(handler, "", port):
        await stop

if __name__ == "__main__":
    asyncio.run(main())
