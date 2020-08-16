#!python3

import os
import sys
import time
import asyncio
import tempfile
import threading
import functools
import contextlib
import concurrent.futures

import requests



CACHE_LIFE = 60 * 10  # 10 minutes
if sys.platform == 'win32':
    HOSTS = r'C:\Windows\System32\Drivers\etc\hosts'
else:
    HOSTS = '/etc/hosts'



class CloudFlareHTTPSDNSProxy(asyncio.protocols.DatagramProtocol):
    url = 'https://cloudflare-dns.com/dns-query'
    headers={
        'Content-Type': 'application/dns-udpwireformat',
        'User-Agent'  : 'Thanks CloudFlare :D Please hire me because I love you!',
    }
        
    def __init__(self, *args, **kwargs):
        self.executor = kwargs.pop('executor')
        self.loop = kwargs.pop('loop')
        self.session = kwargs.pop('session')
        super().__init__(*args, **kwargs)
        self.cache = {}
        self.cache_lock = threading.Lock()


    def connection_made(self, transport):
        self.transport = transport
        
        
    async def _do_query_async(self, request, callback):
        """Asynchronous hook from datagram_received into event loop
        through an executor. All arguments should be partials.
        """
        callback(
            await self.loop.run_in_executor(
                self.executor, request
        ))
    
    
    
    def datagram_received(self, dns_query, addr):
        """When a datagram arrives, schedule it as a DNS query
        to be forwarded to cloudflare.
        """
        #print('\t QUERY:', addr,  dns_query)
        
        # this either goes to cloudflare or returns bytes 
        # immediately from the cache
        request = functools.partial(
            self._resolve_host,
            dns_query
        )
        
        # this will be called from the executor and will
        # result in DNS response bytes (somehow)
        callback = functools.partial(
            self._datagram_reply,
            addr=addr
        )
        
        # pop this babies into the event loop
        self.loop.create_task(self._do_query_async(request, callback))
        
    
    def _datagram_reply(self, dns_reply, addr):
        #print('\tRESPONSE:', addr, dns_reply)
        self.transport.sendto(dns_reply, addr)
        
        
    def _resolve_host(self, dns_query):
        """Queries are stripped of the first two bytes (sequence ID)
        and stored with CloudFlare's response, along with a timestamp
        inside a dict. 
        """
        stripped_query = dns_query[2:]
        cached_result = self.cache.get(stripped_query, None)
        
        # if we have no cached query response, do a query
        if cached_result is None:
            with self.cache_lock:
                resp = self.session.post(
                    self.url,
                    headers=self.headers,
                    data=dns_query
                )
                resp.raise_for_status()

                # strip sequence ID from cached result.
                cached_result = resp.content[2:]
                # add timestamp to cache
                cached_result = (cached_result, int(time.time()))
                self.cache[stripped_query] = cached_result
                
        else:
            # If the cache was hit, update the timestamp and forward from cache
                self.cache[stripped_query] = (cached_result[0], int(time.time()))
            
        # Inject sequence ID back into response
        return dns_query[:2] + cached_result[0]
        
    

async def clean_cache(dns_protocol):
    """Periodically clean cache of stale DNS records."""
    while True:
        await asyncio.sleep(CACHE_LIFE)
        current_time = int(time.time())
        with dns_protocol.cache_lock:
            for k,v in list(dns_protocol.cache.items()):
                if current_time - v[1] > CACHE_LIFE:
                    dns_protocol.cache.pop(k)
            



async def run_server(host, port):
    """This coroutine creates the necessary objects for the 
    event loop to run smoothly, then instantiates the UDP server
    and tries to run forever.
    """
    loop = asyncio.get_event_loop()
    session = requests.Session()
    executor = concurrent.futures.ThreadPoolExecutor(20)
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: CloudFlareHTTPSDNSProxy(
            loop=loop, executor=executor, session=session
        ),
        local_addr=(host, port),
    )
    
    loop.create_task(clean_cache(protocol))
    
    print('Starting server on', f'{host}:{port}')
    run_forever = loop.create_future()
    try:
        await run_forever
    finally:
        print('Shutting down!')
        transport.close()



@contextlib.contextmanager
def update_hosts_file(host_record):
    """Saves the hosts file to a secure temporary file, 
    then appends ``host_record'' to it. When this context
    is closed, the orginal file will be read back, thereby
    *overwriting* the hosts file with old data.
    """
    print('Configuring hosts file...', )
    with tempfile.TemporaryFile('r+b') as saved_hosts:
        with open(HOSTS, 'a+b') as hosts:
            hosts.seek(0)
            txt = hosts.read()
            saved_hosts.write(txt)
            hosts.write(os.linesep.encode() + host_record)
        
        try:
            yield
        finally:
            print('Restoring hosts file...')
            saved_hosts.seek(0)
            with open(HOSTS, 'wb') as hosts:
                hosts.write(saved_hosts.read())
    



def run(host='127.0.0.1', port=5353, priv_port=53):
    is_admin = False

    # check for super powers
    if sys.platform == 'win32':
        import ctypes
        if ctypes.windll.shell32.IsUserAnAdmin():
            is_admin = True
    else:
        if os.geteuid() == 0:
            is_admin = True
    
    try:
        if is_admin:
            print('Running as privileged user.')
            with update_hosts_file(b'1.1.1.1\tcloudflare-dns.com'):
                asyncio.run(run_server(host, priv_port))
        else:
            print('Running as non-privileged user. '
                  'An external DNS server will be required '
                  'to look up cloudflare-dns.com.')
            asyncio.run(run_server(host, port))
    except KeyboardInterrupt:
        print('Done.')



def main():
    run()



if __name__ == '__main__':
    main()
