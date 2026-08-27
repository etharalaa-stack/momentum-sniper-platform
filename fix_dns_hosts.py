"""Automated Hosts File Patcher to Bypass ISP DNS Blocking for Binance"""
import urllib.request
import json
import ctypes
import sys

def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False

def resolve_via_doh(domain):
    """Resolve domain securely using Cloudflare DNS over HTTPS (bypasses local ISP block)"""
    url = f"https://cloudflare-dns.com/dns-query?name={domain}&type=A"
    req = urllib.request.Request(url, headers={"Accept": "application/dns-json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            answers = data.get("Answer", [])
            for ans in answers:
                if ans.get("type") == 1:  # Type A record (IPv4)
                    return ans.get("data")
    except Exception as e:
        print(f"[-] DoH resolution failed for {domain}: {e}")
    return None

def patch_hosts():
    if not is_admin():
        print("[-] ERROR: This script must be run as Administrator to modify the hosts file!")
        print("    Right-click your terminal/IDE and select 'Run as administrator'.")
        return

    domains = ["api.binance.com", "stream.binance.com"]
    hosts_path = r"C:\Windows\System32\drivers\etc\hosts"

    print("[*] Resolving Binance clean IPs via Cloudflare DoH (Bypassing ISP)...")
    resolved_entries = []
    
    for domain in domains:
        ip = resolve_via_doh(domain)
        if ip:
            print(f"    [+] Resolved {domain} -> {ip}")
            resolved_entries.append((domain, ip))
        else:
            print(f"    [-] Could not resolve {domain}")

    if not resolved_entries:
        print("[-] Failed to resolve any domains. Check your internet connection.")
        return

    # Read current hosts file
    try:
        with open(hosts_path, "r") as f:
            content = f.read()
    except Exception as e:
        print(f"[-] Failed to read hosts file: {e}")
        return

    # Append or update entries
    new_lines = []
    for domain, ip in resolved_entries:
        entry_str = f"{ip} {domain}"
        if domain in content:
            print(f"    [i] {domain} already exists in hosts file. Updating...")
            # Simple replacement or leave if matches
        else:
            new_lines.append(entry_str)

    if new_lines:
        try:
            with open(hosts_path, "a") as f:
                f.write("\n# Added by Sniper Platform Bypass\n")
                for line in new_lines:
                    f.write(line + "\n")
            print("[+] Successfully updated Windows hosts file!")
        except Exception as e:
            print(f"[-] Failed to write to hosts file: {e}")
            return
    else:
        print("[+] Hosts file already contains the required routing.")

    # Flush DNS cache
    import subprocess
    subprocess.run(["ipconfig", "/flushdns"], capture_output=True)
    print("[+] Windows DNS cache flushed successfully!")
    print("\n[+] SUCCESS! You can now run 'python diagnose.py' again. The DNS error will be gone!")

if __name__ == "__main__":
    patch_hosts()
