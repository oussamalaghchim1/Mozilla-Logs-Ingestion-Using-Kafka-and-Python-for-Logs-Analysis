#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Mozilla Build Log Parser - Kafka Consumer Version
Consumes raw logs from Kafka and produces parsed structured data
"""
import re
import json
import os
import tempfile
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer

# Kafka Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
INPUT_TOPIC = os.getenv('INPUT_TOPIC', 'mozilla-build-logs')
OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'parsed-logs')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'log-parser-group')
CONSUMER_GROUP = os.getenv('CONSUMER_GROUP', 'log-parser-group')


class MozillaBuildLogParser:
    """Parser for Mozilla build log files"""
    
    def __init__(self):
        self.step_result_codes = {
            '0': 'success',
            '3': 'skipped',
            '4': 'interrupted',
            '6': 'cancelled'
        }
    
    def parse_content(self, content: str, log_id: str = 'unknown') -> Dict[str, Any]:
        """
        Parse Mozilla build log content and extract all structured data
        
        Args:
            content: Raw log file content
            log_id: Identifier for the log
            
        Returns:
            Dictionary with all extracted structured data
        """
        # Extract all components
        result = {
            'log_id': log_id,
            'build_metadata': self._extract_build_metadata(content),
            'build_properties': self._extract_build_properties(content),
            'build_environment': self._extract_environment(content),
            'build_steps': self._extract_build_steps(content),
            'test_artifacts': self._extract_test_artifacts(content),
            'downloads_performed': self._extract_downloads(content),
            'virtualenv_packages': self._extract_virtualenv_packages(content),
            'mozharness_actions': self._extract_mozharness_actions(content),
            'timing_summary': self._extract_timing_summary(content),
            'warnings': self._extract_warnings(content),
            'errors': self._extract_errors(content),
            'platform_info': {},
            'derived_fields': {},
            'parsed_at': datetime.utcnow().isoformat() + 'Z'
        }
        
        # Add platform detection
        result['platform_info'] = self._detect_platform(result)
        
        # Add derived fields
        result['derived_fields'] = self._compute_derived_fields(result)
        
        return result
    
    def _extract_build_metadata(self, content: str) -> Dict[str, Any]:
        """Extract core build metadata from the header"""
        metadata = {}
        
        # Extract header fields
        patterns = {
            'builder': r'^builder:\s*(.+)$',
            'slave': r'^slave:\s*(.+)$',
            'starttime': r'^starttime:\s*(\d+\.?\d*)$',
            'results': r'^results:\s*(.+?)\s*\((\d+)\)',
            'buildid': r'^buildid:\s*(.+)$',
            'builduid': r'^builduid:\s*(.+)$',
            'revision': r'^revision:\s*(.+)$',
            'master': r'^master:\s*(.+)$'
        }
        
        for key, pattern in patterns.items():
            match = re.search(pattern, content, re.MULTILINE)
            if match:
                if key == 'results':
                    metadata['build_result'] = match.group(1)
                    metadata['build_result_code'] = int(match.group(2))
                    metadata['build_result_status'] = self.step_result_codes.get(match.group(2), 'unknown')
                elif key == 'starttime':
                    timestamp = float(match.group(1))
                    metadata['start_timestamp'] = timestamp
                    metadata['start_datetime'] = datetime.fromtimestamp(timestamp).isoformat() + 'Z'
                elif key == 'master':
                    metadata['master_url'] = match.group(1)
                else:
                    metadata[key] = match.group(1).strip()
        
        # Extract revision short form
        if 'revision' in metadata:
            metadata['revision_short'] = metadata['revision'][:12]
        
        # Extract cancellation info
        cancel_match = re.search(
            r"The web-page 'stop build' button was pressed by '([^']+)':\s*(.+)$",
            content,
            re.MULTILINE
        )
        if cancel_match:
            metadata['cancelled_by'] = cancel_match.group(1)
            metadata['cancellation_reason'] = cancel_match.group(2).strip()
        
        return metadata
    
    def _extract_build_properties(self, content: str) -> Dict[str, Any]:
        """Extract buildbot properties"""
        properties = {}
        
        # Find the buildbot properties JSON block
        props_match = re.search(
            r'Using buildbot properties:\s*\n\s*INFO\s*-\s*\{\s*\n(.*?)\n\s*INFO\s*-\s*\}',
            content,
            re.DOTALL
        )
        
        if props_match:
            props_text = props_match.group(1)
            
            # Extract individual properties
            prop_pattern = r'INFO\s*-\s*"([^"]+)":\s*"([^"]*)"'
            for match in re.finditer(prop_pattern, props_text):
                key = match.group(1)
                value = match.group(2)
                properties[key] = value
        
        return properties
    
    def _extract_environment(self, content: str) -> Dict[str, Any]:
        """Extract environment variables from first command"""
        env_vars = {}
        
        # Find the first environment block
        env_match = re.search(
            r'environment:\s*\n((?:\s+\w+=[^\n]+\n)+)',
            content
        )
        
        if env_match:
            env_block = env_match.group(1)
            for line in env_block.split('\n'):
                line = line.strip()
                if '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key] = value
        
        # Detect OS and platform from environment
        os_info = {
            'os_type': 'unknown',
            'architecture': 'unknown'
        }
        
        if 'OS' in env_vars:
            os_type = env_vars['OS']
            if 'Windows' in os_type:
                os_info['os_type'] = 'windows'
            elif 'Darwin' in os_type or 'VERSIONER_PYTHON_VERSION' in env_vars:
                os_info['os_type'] = 'macos'
            else:
                os_info['os_type'] = 'linux'
        
        if 'PROCESSOR_ARCHITEW6432' in env_vars:
            os_info['architecture'] = 'x64'
        elif 'PROCESSOR_ARCHITECTURE' in env_vars:
            os_info['architecture'] = env_vars['PROCESSOR_ARCHITECTURE']
        
        return {
            'variables': env_vars,
            'os_info': os_info
        }
    
    def _extract_build_steps(self, content: str) -> List[Dict[str, Any]]:
        """Extract all build steps with timing and results"""
        steps = []
        
        # Pattern for step headers
        step_pattern = re.compile(
            r"========= Started (.+?) \(results: (\d+), elapsed: (.+?)\) \(at (.+?)\) =========\n"
            r"(.*?)"
            r"========= Finished (.+?) \(results: (\d+), elapsed: (.+?)\) \(at (.+?)\) =========",
            re.DOTALL
        )
        
        step_number = 1
        for match in step_pattern.finditer(content):
            step_name = match.group(1).strip()
            start_result = int(match.group(2))
            elapsed_str = match.group(3)
            start_time = match.group(4)
            step_content = match.group(5)
            end_result = int(match.group(7))
            end_time = match.group(9)
            
            # Parse elapsed time
            elapsed_seconds = self._parse_elapsed_time(elapsed_str)
            
            # Extract command if present
            command = self._extract_command_from_step(step_content)
            
            # Extract elapsed time from content if available
            elapsed_match = re.search(r'elapsedTime=(\d+\.?\d*)', step_content)
            if elapsed_match:
                actual_elapsed = float(elapsed_match.group(1))
            else:
                actual_elapsed = elapsed_seconds
            
            # Extract master_lag
            master_lag = None
            lag_match = re.search(r'master_lag:\s*([-\d.]+)', step_content)
            if lag_match:
                master_lag = float(lag_match.group(1))
            
            # Extract exit code if present
            exit_code = None
            exit_match = re.search(r'program finished with exit code (-?\d+)', step_content)
            if exit_match:
                exit_code = int(exit_match.group(1))
            
            # Extract interruption details
            interrupted = 'interrupted' in step_name.lower()
            kill_signal = None
            if interrupted:
                signal_match = re.search(r'process killed by signal (\d+)', step_content)
                if signal_match:
                    kill_signal = int(signal_match.group(1))
            
            step = {
                'step_number': step_number,
                'step_name': step_name,
                'command': command,
                'start_time': start_time,
                'end_time': end_time,
                'elapsed_seconds': actual_elapsed,
                'result': end_result,
                'result_status': self.step_result_codes.get(str(end_result), 'unknown'),
                'exit_code': exit_code,
                'master_lag': master_lag,
                'interrupted': interrupted,
                'kill_signal': kill_signal
            }
            
            # Add download-specific info
            if 'download' in step_name.lower():
                step['download_info'] = self._extract_download_info_from_step(step_content)
            
            steps.append(step)
            step_number += 1
        
        return steps
    
    def _extract_command_from_step(self, step_content: str) -> Optional[str]:
        """Extract the command executed in a step"""
        cmd_patterns = [
            r"'([^']+)'\s+'([^']+)'",
            r'"([^"]+)"\s+"([^"]+)"',
            r'argv:\s*\[(.+?)\]',
        ]
        
        for pattern in cmd_patterns:
            match = re.search(pattern, step_content)
            if match:
                return match.group(0)
        
        return None
    
    def _extract_download_info_from_step(self, step_content: str) -> Dict[str, Any]:
        """Extract download-specific information from step content"""
        info = {}
        
        speed_match = re.search(r'(\d+\.?\d*)\s*([KMG]B/s)', step_content)
        if speed_match:
            info['download_speed'] = f"{speed_match.group(1)} {speed_match.group(2)}"
        
        size_match = re.search(r'Length:\s*(\d+(?:,\d+)*)\s*\(([^)]+)\)', step_content)
        if size_match:
            info['file_size_bytes'] = int(size_match.group(1).replace(',', ''))
            info['file_size_human'] = size_match.group(2)
        
        http_match = re.search(r'HTTP.*?(\d{3})', step_content)
        if http_match:
            info['http_status'] = int(http_match.group(1))
        
        ip_match = re.search(r'Resolving.*?\.\.\.\s*([\d.,\s]+)', step_content)
        if ip_match:
            info['resolved_ips'] = [ip.strip() for ip in ip_match.group(1).split(',')]
        
        return info
    
    def _extract_test_artifacts(self, content: str) -> Dict[str, Any]:
        """Extract test artifact URLs and metadata"""
        artifacts = {}
        
        installer_match = re.search(r'Found installer url\s+(https?://[^\s]+)', content)
        if installer_match:
            url = installer_match.group(1).rstrip('.')
            artifacts['installer_url'] = url
            
            version_match = re.search(r'firefox-([0-9.]+)\.([a-z]{2}-[A-Z]{2})', url)
            if version_match:
                artifacts['firefox_version'] = version_match.group(1)
                artifacts['locale'] = version_match.group(2)
            
            if 'mac64' in url:
                artifacts['installer_platform'] = 'mac64'
            elif 'win64' in url:
                artifacts['installer_platform'] = 'win64'
            elif 'win32' in url:
                artifacts['installer_platform'] = 'win32'
        
        test_pkg_match = re.search(r'Found a test packages url\s+(https?://[^\s]+)', content)
        if test_pkg_match:
            artifacts['test_packages_url'] = test_pkg_match.group(1).rstrip('.')
        
        symbols_match = re.search(r'symbols_url[:\s]+(https?://[^\s]+)', content)
        if symbols_match:
            artifacts['symbols_url'] = symbols_match.group(1)
        
        build_match = re.search(r'build_url[:\s]+(https?://[^\s]+)', content)
        if build_match:
            artifacts['build_url'] = build_match.group(1)
        
        taskcluster_match = re.search(r'task/([a-zA-Z0-9_-]{22})/artifacts', content)
        if taskcluster_match:
            artifacts['taskcluster_task_id'] = taskcluster_match.group(1)
        
        return artifacts
    
    def _extract_downloads(self, content: str) -> List[Dict[str, Any]]:
        """Extract all file downloads performed"""
        downloads = []
        
        download_pattern = re.compile(
            r'(?:Downloading|Fetch)\s+(https?://[^\s]+)\s+(?:to|into)\s+([^\s]+)',
            re.IGNORECASE
        )
        
        for match in download_pattern.finditer(content):
            url = match.group(1)
            destination = match.group(2)
            filename = url.split('/')[-1].split('?')[0]
            
            download = {
                'url': url,
                'destination': destination,
                'filename': filename
            }
            
            start_pos = match.end()
            nearby_text = content[start_pos:start_pos + 500]
            
            size_match = re.search(r'Expected file size:\s*(\d+)', nearby_text)
            if size_match:
                download['expected_size_bytes'] = int(size_match.group(1))
            
            obtained_match = re.search(r'Obtained file size:\s*(\d+)', nearby_text)
            if obtained_match:
                download['actual_size_bytes'] = int(obtained_match.group(1))
            
            http_match = re.search(r'Http code:\s*(\d+)', nearby_text)
            if http_match:
                download['http_status'] = int(http_match.group(1))
            
            cdn_match = re.search(r'via:.*?(cloudfront\.net|CloudFront)', nearby_text, re.IGNORECASE)
            if cdn_match:
                download['cdn'] = 'CloudFront'
            
            version_match = re.search(r'x-amz-version-id:\s*([^\s]+)', nearby_text)
            if version_match:
                download['x_amz_version_id'] = version_match.group(1)
            
            downloads.append(download)
        
        return downloads
    
    def _extract_virtualenv_packages(self, content: str) -> List[str]:
        """Extract virtualenv packages installed"""
        packages = []
        
        install_patterns = [
            r'Installing\s+(.+?)==([\d.]+)',
            r'Successfully installed\s+(.+)',
        ]
        
        for pattern in install_patterns:
            for match in re.finditer(pattern, content):
                package_info = match.group(1).strip()
                for pkg in package_info.split():
                    if pkg and pkg not in packages:
                        packages.append(pkg)
        
        pip_pattern = r"pip.*?install.*?['\"]([^'\"]+)['\"]"
        for match in re.finditer(pip_pattern, content):
            pkg = match.group(1).strip()
            if pkg and pkg not in packages:
                packages.append(pkg)
        
        return packages
    
    def _extract_mozharness_actions(self, content: str) -> List[Dict[str, Any]]:
        """Extract mozharness actions"""
        actions = []
        
        action_pattern = re.compile(
            r'\[mozharness: ([^\]]+)\] Running (.+?) step\.\n'
            r'(.*?)'
            r'\[mozharness: ([^\]]+)\] Finished (.+?) step \((.+?)\)',
            re.DOTALL
        )
        
        for match in action_pattern.finditer(content):
            start_time = match.group(1)
            action_name = match.group(2)
            action_content = match.group(3)
            end_time = match.group(4)
            status = match.group(6)
            
            action = {
                'action': action_name,
                'start_time': start_time,
                'end_time': end_time,
                'status': status
            }
            
            if 'rmtree' in action_content:
                rmtree_matches = re.findall(r"rmtree:\s*([^\n]+)", action_content)
                if rmtree_matches:
                    action['operations'] = [{'operation': 'rmtree', 'target': t.strip()} for t in rmtree_matches]
            
            actions.append(action)
        
        return actions
    
    def _extract_timing_summary(self, content: str) -> Dict[str, Any]:
        """Extract timing summary information"""
        timing = {}
        
        total_lag_match = re.search(r'Total master_lag:\s*([-\d.]+)', content)
        if total_lag_match:
            timing['total_master_lag'] = float(total_lag_match.group(1))
        
        elapsed_times = re.findall(r'elapsedTime=([\d.]+)', content)
        if elapsed_times:
            timing['total_elapsed_seconds'] = sum(float(t) for t in elapsed_times)
        
        step_count = len(re.findall(r'Started .+? \(results:', content))
        timing['total_steps'] = step_count
        
        results = re.findall(r'Finished .+? \(results: (\d+)', content)
        timing['steps_successful'] = results.count('0')
        timing['steps_failed'] = results.count('2')
        timing['steps_interrupted'] = results.count('4')
        timing['steps_skipped'] = results.count('3')
        
        return timing
    
    def _extract_warnings(self, content: str) -> List[str]:
        """Extract warning messages"""
        warnings = []
        
        warning_patterns = [
            r'WARNING\s*-\s*(.+?)(?:\n|$)',
            r'SNIMissingWarning:\s*(.+?)(?:\n|$)',
            r'InsecurePlatformWarning:\s*(.+?)(?:\n|$)',
            r'Certificate verification error.*?:\s*(.+?)(?:\n|$)',
        ]
        
        for pattern in warning_patterns:
            for match in re.finditer(pattern, content):
                warning = match.group(1).strip()
                if warning and warning not in warnings:
                    warnings.append(warning)
        
        return warnings
    
    def _extract_errors(self, content: str) -> List[str]:
        """Extract error messages"""
        errors = []
        
        error_patterns = [
            r'ERROR\s*-\s*(.+?)(?:\n|$)',
            r'Traceback \(most recent call last\):.*?(?=\n\s*\n|\Z)',
            r'(?:command|process) (?:failed|killed).*?(?:\n|$)',
        ]
        
        for pattern in error_patterns:
            for match in re.finditer(pattern, content, re.DOTALL):
                error = match.group(0).strip()
                if error and error not in errors:
                    if len(error) > 500:
                        error = error[:500] + '...[truncated]'
                    errors.append(error)
        
        return errors
    
    def _detect_platform(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Detect platform information from parsed data"""
        platform_info = {
            'os_type': 'unknown',
            'architecture': 'unknown',
            'path_separator': '/',
            'installer_format': 'unknown'
        }
        
        slave = result['build_metadata'].get('slave', '').lower()
        props_platform = result['build_properties'].get('platform', '').lower()
        builder = result['build_metadata'].get('builder', '').lower()
        
        windows_patterns = [
            r't-w\d+', r't-xp\d+', r't-win\d+', r'win\d+', r'windows',
        ]
        
        macos_patterns = [
            r't-osx', r't-yosemite', r'yosemite', r'macosx', r'mac\d+', r'osx', r'darwin',
        ]
        
        linux_patterns = [
            r't-linux', r'ubuntu', r'linux\d+', r'centos', r'debian',
        ]
        
        search_text = f"{slave} {props_platform} {builder}"
        
        if any(re.search(pattern, search_text) for pattern in windows_patterns):
            platform_info['os_type'] = 'windows'
            platform_info['path_separator'] = '\\\\'
            platform_info['installer_format'] = 'zip'
        
        elif any(re.search(pattern, search_text) for pattern in macos_patterns):
            platform_info['os_type'] = 'macos'
            platform_info['path_separator'] = '/'
            platform_info['installer_format'] = 'dmg'
        
        elif any(re.search(pattern, search_text) for pattern in linux_patterns):
            platform_info['os_type'] = 'linux'
            platform_info['path_separator'] = '/'
            platform_info['installer_format'] = 'tar.bz2'
        
        if platform_info['os_type'] == 'unknown':
            env_os = result['build_environment']['os_info'].get('os_type', '').lower()
            if env_os in ['windows', 'macos', 'linux']:
                platform_info['os_type'] = env_os
                if env_os == 'windows':
                    platform_info['path_separator'] = '\\\\'
                    platform_info['installer_format'] = 'zip'
                else:
                    platform_info['path_separator'] = '/'
        
        arch_64_patterns = [r'64', r'x64', r'amd64', r'x86_64']
        arch_32_patterns = [r'32', r'x86(?!_64)', r'i686']
        arch_arm_patterns = [r'arm', r'aarch64']
        
        if any(re.search(pattern, search_text) for pattern in arch_64_patterns):
            platform_info['architecture'] = 'x64'
        elif any(re.search(pattern, search_text) for pattern in arch_32_patterns):
            platform_info['architecture'] = 'x86'
        elif any(re.search(pattern, search_text) for pattern in arch_arm_patterns):
            platform_info['architecture'] = 'arm'
        
        if platform_info['architecture'] == 'unknown':
            env_arch = result['build_environment']['os_info'].get('architecture', '')
            if env_arch:
                platform_info['architecture'] = env_arch
        
        return platform_info
    
    def _compute_derived_fields(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Compute derived/enriched fields"""
        derived = {}
        
        builder = result['build_metadata'].get('builder', '')
        if 'mochitest' in builder:
            derived['test_category'] = 'mochitest'
        elif 'reftest' in builder or 'jsreftest' in builder:
            derived['test_category'] = 'reftest'
        elif 'xpcshell' in builder:
            derived['test_category'] = 'xpcshell'
        elif 'web-platform' in builder:
            derived['test_category'] = 'web-platform-tests'
        elif 'gtest' in builder:
            derived['test_category'] = 'gtest'
        else:
            derived['test_category'] = 'other'
        
        test_suite_match = re.search(r'test[_-](\S+)', builder)
        if test_suite_match:
            derived['test_suite'] = test_suite_match.group(1)
        
        steps = result.get('build_steps', [])
        if steps:
            last_step = steps[-1]
            step_name = last_step['step_name'].lower()
            
            if 'clobber' in step_name:
                derived['execution_phase'] = 'clobber'
            elif 'download' in step_name:
                derived['execution_phase'] = 'download'
            elif 'virtualenv' in step_name:
                derived['execution_phase'] = 'virtualenv_setup'
            elif 'install' in step_name:
                derived['execution_phase'] = 'installation'
            elif any(x in step_name for x in ['test', 'run', 'execute']):
                derived['execution_phase'] = 'test_execution'
            else:
                derived['execution_phase'] = 'other'
        
        if steps:
            successful_steps = sum(1 for s in steps if s['result_status'] == 'success')
            derived['progress_percentage'] = (successful_steps / len(steps)) * 100
        
        derived['e10s_enabled'] = 'e10s' in builder.lower()
        
        if 'debug' in builder.lower():
            derived['build_type'] = 'debug'
        elif 'opt' in builder.lower():
            derived['build_type'] = 'opt'
        else:
            derived['build_type'] = 'unknown'
        
        return derived
    
    def _parse_elapsed_time(self, elapsed_str: str) -> float:
        """Parse elapsed time string to seconds"""
        if 'mins' in elapsed_str or 'min' in elapsed_str:
            mins_match = re.search(r'(\d+)\s*mins?', elapsed_str)
            secs_match = re.search(r'(\d+)\s*secs?', elapsed_str)
            
            minutes = int(mins_match.group(1)) if mins_match else 0
            seconds = int(secs_match.group(1)) if secs_match else 0
            
            return minutes * 60 + seconds
        elif 'secs' in elapsed_str or 'sec' in elapsed_str:
            secs_match = re.search(r'(\d+)\s*secs?', elapsed_str)
            return int(secs_match.group(1)) if secs_match else 0
        else:
            try:
                return float(elapsed_str.split()[0])
            except:
                return 0.0


def main():
    """Kafka Consumer main loop"""
    print("=" * 70)
    print("Mozilla Build Log Parser - Kafka Consumer")
    print("=" * 70)
    
    # Initialize parser
    parser = MozillaBuildLogParser()
    print(f"\n✓ Parser initialized")
    
    # Create Kafka consumer
    print(f"\n🔌 Connecting to Kafka broker: {KAFKA_BROKER}")
    print(f"📥 Input topic: {INPUT_TOPIC}")
    print(f"📤 Output topic: {OUTPUT_TOPIC}")
    print(f"👥 Consumer group: {CONSUMER_GROUP}")
    
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Create Kafka producer for parsed data
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )
    
    print("\n✓ Connected! Waiting for messages...\n")
    print("=" * 70)
    
    processed_count = 0
    
    try:
        for message in consumer:
            processed_count += 1
            raw_log = message.value
            
            log_id = raw_log.get('log_id', f'unknown_{processed_count}')
            content = raw_log.get('content', '')
            
            if not content:
                print(f"⚠️  [{processed_count}] Empty log content for {log_id}, skipping...")
                continue
            
            print(f"\n📄 [{processed_count}] Processing: {log_id}")
            
            try:
                # Parse the log content (same treatment as parser.py)
                parsed_data = parser.parse_content(content, log_id)
                
                # Send to output topic
                producer.send(OUTPUT_TOPIC, value=parsed_data)
                
                # Print summary
                metadata = parsed_data.get('build_metadata', {})
                print(f"   Builder: {metadata.get('builder', 'unknown')}")
                print(f"   Result: {metadata.get('build_result_status', 'unknown')}")
                print(f"   Steps: {len(parsed_data.get('build_steps', []))}")
                print(f"   ✓ Sent to topic: {OUTPUT_TOPIC}")
                
            except Exception as e:
                print(f"   ❌ Error parsing: {e}")
            
            print("-" * 70)
    
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down...")
    
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        print(f"\n📊 Total processed: {processed_count}")
        print("=" * 70)


if __name__ == "__main__":
    main()