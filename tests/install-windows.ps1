param([string]$BinaryPath = 'target/debug/mssh.exe')
$ErrorActionPreference = 'Stop'

$root = Join-Path ([IO.Path]::GetTempPath()) ('mssh-installer-' + [guid]::NewGuid().ToString('N'))
$bin = Join-Path $root 'bin'
$configDir = Join-Path $root 'config'
$config = Join-Path $configDir 'config.toml'
$sourceBinary = (Resolve-Path -LiteralPath $BinaryPath).Path
$sourceExample = (Resolve-Path -LiteralPath 'examples/config.toml').Path
$installer = (Resolve-Path -LiteralPath 'scripts/install.ps1').Path
New-Item -ItemType Directory -Path $root | Out-Null
$env:MSSH_INSTALL_DIR = $bin
$env:MSSH_CONFIG_DIR = $configDir
$env:MSSH_RELEASE_URL = 'test://binary'
$env:MSSH_EXAMPLE_URL = 'test://example'
$env:MSSH_SKIP_PATH_UPDATE = '1'

function Invoke-WebRequest {
    param($Uri, $OutFile)
    switch ($Uri) {
        'test://binary' { Copy-Item -LiteralPath $sourceBinary -Destination $OutFile }
        'test://example' { Copy-Item -LiteralPath $sourceExample -Destination $OutFile }
        default { throw "Unexpected URL: $Uri" }
    }
}

try {
    & $installer
    if (-not (Test-Path -LiteralPath (Join-Path $bin 'mssh.exe'))) { throw 'Binary was not installed' }
    if (-not (Test-Path -LiteralPath $config)) { throw 'Example config was not installed' }
    Set-Content -LiteralPath $config -Value 'custom config'
    & $installer
    if ((Get-Content -LiteralPath $config -Raw).Trim() -ne 'custom config') {
        throw 'Existing config was overwritten'
    }
    Write-Host 'Windows installer test passed'
} finally {
    Remove-Item Env:MSSH_INSTALL_DIR, Env:MSSH_CONFIG_DIR, Env:MSSH_RELEASE_URL, Env:MSSH_EXAMPLE_URL, Env:MSSH_SKIP_PATH_UPDATE -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $bin 'mssh.exe'), $config -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $bin, $configDir, $root -Force -ErrorAction SilentlyContinue
}
